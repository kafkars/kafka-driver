import { expect, smoke } from "smoque";

import {
  awaitBroker,
  startBroker,
  stopBroker,
  upBroker,
} from "./support/kafka-cluster.mjs";
import { requireDeadFirstLocalhost } from "./support/localhost-addresses.mjs";

const PROBE = process.platform === "win32" ? "kafka-driver-probe.exe" : "kafka-driver-probe";

smoke.suite("real Kafka multi-broker failover", { tags: ["real-kafka-multi-broker"] }, async (t) => {
  const root = t.repoRoot();
  const composeFile = root.path("smoke", "kafka-cluster.compose.yml");
  const probe = root.path("target", "debug", PROBE);
  const coordination = await t.tempDir("kafka-driver-rolling");
  const dead = await t.ports.reserve("kafka-dead-bootstrap");
  const first = await t.ports.reserve("kafka-1");
  const second = await t.ports.reserve("kafka-2");
  const third = await t.ports.reserve("kafka-3");
  const deadFirstEndpoints = [dead, first]
    .map((port) => `${port.host}:${port.port}`)
    .join(",");
  const rollingEndpoints = [first, second]
    .map((port) => `${port.host}:${port.port}`)
    .join(",");
  const composeEnv = {
    KAFKA_1_HOST_PORT: String(first.port),
    KAFKA_2_HOST_PORT: String(second.port),
    KAFKA_3_HOST_PORT: String(third.port),
  };
  const docker = await t.tools.docker();

  await t.step("required tools are available", async () => {
    await t.tools.node({ minVersion: "22.18.0" });
    await t.cmd("cargo", ["--version"], { cwd: root, timeout: "10s" });
    await t.compose.check({ docker: docker.command, cwd: root, timeout: "10s" });
  });

  await t.step("build the public-surface probe", async () => {
    await t.cmd("cargo", ["build", "--locked", "-p", "kafka-driver-probe"], {
      cwd: root,
      timeout: "2m",
    });
    await expect.file(probe).toExist();
  });

  const stack = await t.step("start brokers 1 and 3 as a quorum", async () => {
    return await t.compose.up({
      docker: docker.command,
      cwd: root,
      file: composeFile,
      env: composeEnv,
      services: ["kafka-1", "kafka-3"],
      timeout: "6m",
    });
  });

  await t.step("require the initial quorum to be ready", async () => {
    for (const broker of ["kafka-1", "kafka-3"]) {
      await awaitBroker(t, docker.command, stack, composeFile, composeEnv, broker);
    }
  });

  const deadFirstHost = await t.step("require dead-first A/AAAA resolver order", async () => {
    return await requireDeadFirstLocalhost(first.port);
  });

  await t.step("rotate one logical host past its refused first address", async () => {
    const result = await t.cmd(probe, ["dns-rotation", deadFirstHost], {
      cwd: root,
      timeout: "30s",
    });
    expect.value(result.stdout).toContain("PASS dead-first DNS address rotation");
  });

  await t.step("skip one dead bootstrap endpoint", async () => {
    const result = await t.cmd(probe, ["readiness", deadFirstEndpoints], {
      cwd: root,
      timeout: "30s",
    });
    expect.value(result.stdout).toContain("PASS any-broker ApiVersions");
  });

  const rolling = await t.step("start one ordered rolling session", async () => {
    return await t.process.start(probe, ["rolling", rollingEndpoints, coordination.toString()], {
      cwd: root,
      name: "kafka-driver-rolling-probe",
      ready: t.log.contains("READY initial multi-broker connection", { stream: "stdout" }),
      timeout: "45s",
    });
  });

  await t.step("bring broker 2 online as the first failover target", async () => {
    await upBroker(t, docker.command, stack, composeFile, composeEnv, "kafka-2");
    await awaitBroker(t, docker.command, stack, composeFile, composeEnv, "kafka-2");
  });

  await t.step("lose broker 1 and fail over to broker 2", async () => {
    await stopBroker(t, docker.command, stack, composeFile, composeEnv, "kafka-1");
    await t.fs.writeText(coordination.path("broker-1-stopped"), "stopped\n");
    await requireOutput(t, rolling, "RECOVERED rolling broker failover 1");
  });

  await t.step("restore broker 1 before the next rolling loss", async () => {
    await startBroker(t, docker.command, stack, composeFile, composeEnv, "kafka-1");
    await awaitBroker(t, docker.command, stack, composeFile, composeEnv, "kafka-1");
  });

  await t.step("lose broker 2 and fail over to restored broker 1", async () => {
    await stopBroker(t, docker.command, stack, composeFile, composeEnv, "kafka-2");
    await t.fs.writeText(coordination.path("broker-2-stopped"), "stopped\n");
    await requireOutput(t, rolling, "PASS rolling broker failover 2");
  });

  await t.step("restore broker 2", async () => {
    await startBroker(t, docker.command, stack, composeFile, composeEnv, "kafka-2");
    await awaitBroker(t, docker.command, stack, composeFile, composeEnv, "kafka-2");
  });

  await rolling.stop();
});

async function requireOutput(t, process, expected) {
  await t.poll(
    expected,
    async () => {
      if (process.stderr()) {
        throw new Error(process.stderr());
      }
      expect.value(process.stdout()).toContain(expected);
      return process.stdout();
    },
    { timeout: "2m", interval: "250ms" },
  );
}
