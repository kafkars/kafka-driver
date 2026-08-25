import { expect, smoke } from "smoque";

import {
  awaitBroker,
  composeCommand,
  stopBroker,
  upBroker,
} from "./support/kafka-cluster.mjs";

const PROBE = process.platform === "win32" ? "kafka-driver-probe.exe" : "kafka-driver-probe";
const TOPIC = "kafka-driver-movement";

smoke.suite("real Kafka advertised broker movement", { tags: ["real-kafka-functional", "real-kafka-movement"] }, async (t) => {
  const root = t.repoRoot();
  const composeFile = root.path("smoke", "kafka-cluster.compose.yml");
  const probe = root.path("target", "debug", PROBE);
  const coordination = await t.tempDir("kafka-driver-movement");
  const first = await t.ports.reserve("kafka-1-initial");
  const movedFirst = await t.ports.reserve("kafka-1-moved");
  const second = await t.ports.reserve("kafka-2");
  const third = await t.ports.reserve("kafka-3-stable-seed");
  const initialEnv = clusterEnv(first, second, third);
  const movedEnv = clusterEnv(movedFirst, second, third);
  const stableBootstrap = `${third.host}:${third.port}`;
  const movedBootstrap = `${movedFirst.host}:${movedFirst.port}`;
  const docker = await t.tools.docker();

  await t.step("required tools are available", async () => {
    await t.tools.node({ minVersion: "22.18.0" });
    await t.cmd("cargo", ["--version"], { cwd: root, timeout: "10s" });
    await t.compose.check({ docker: docker.command, cwd: root, timeout: "10s" });
  });

  await t.step("build the public-surface movement probe", async () => {
    await t.cmd("cargo", ["build", "--locked", "-p", "kafka-driver-probe"], {
      cwd: root,
      timeout: "2m",
    });
    await expect.file(probe).toExist();
  });

  const stack = await t.step("start the three-broker cluster", async () => {
    return await t.compose.up({
      docker: docker.command,
      cwd: root,
      file: composeFile,
      env: initialEnv,
      services: ["kafka-1", "kafka-2", "kafka-3"],
      timeout: "6m",
    });
  });

  await t.step("require every initial broker to be ready", async () => {
    for (const broker of ["kafka-1", "kafka-2", "kafka-3"]) {
      await awaitBroker(t, docker.command, stack, composeFile, initialEnv, broker);
    }
  });

  await t.step("create one partition led only by broker 1", async () => {
    await composeCommand(t, docker.command, stack, composeFile, initialEnv, [
      "exec",
      "--no-TTY",
      "kafka-3",
      "/opt/kafka/bin/kafka-topics.sh",
      "--bootstrap-server",
      "127.0.0.1:19092",
      "--create",
      "--if-not-exists",
      "--topic",
      TOPIC,
      "--replica-assignment",
      "1",
    ]);
  });

  const movement = await t.step("start one tracked partition session", async () => {
    return await t.process.start(
      probe,
      ["movement", stableBootstrap, TOPIC, coordination.toString()],
      {
        cwd: root,
        name: "kafka-driver-movement-probe",
        ready: t.log.contains("READY initial advertised broker route", { stream: "stdout" }),
        timeout: "45s",
      },
    );
  });

  await t.step("stop broker 1 and require the old registration to be fenced", async () => {
    await stopBroker(t, docker.command, stack, composeFile, initialEnv, "kafka-1", "30");
    await requireMetadata(
      t,
      probe,
      root,
      stack,
      ["metadata-fenced", stableBootstrap, TOPIC, "1"],
      "old broker registration fencing",
      "PASS authoritative broker fencing",
    );
  });

  await t.step("recreate broker 1 at a new advertised host port", async () => {
    await upBroker(
      t,
      docker.command,
      stack,
      composeFile,
      movedEnv,
      "kafka-1",
      ["--force-recreate"],
    );
    await awaitBroker(t, docker.command, stack, composeFile, movedEnv, "kafka-1");
  });

  await t.step("require the moved endpoint in live cluster metadata", async () => {
    await requireMetadata(
      t,
      probe,
      root,
      stack,
      ["metadata", stableBootstrap, TOPIC, "1", movedBootstrap],
      "moved broker and leader metadata",
      "PASS authoritative movement metadata",
    );
  });

  await t.step("require a fresh public route through the moved endpoint", async () => {
    const result = await t.cmd(
      probe,
      ["routes", movedBootstrap, TOPIC, "kafka-driver-movement-registration"],
      { cwd: root, timeout: "30s" },
    );
    expect.value(result.stdout).toContain("PASS partition-leader route");
  });

  await t.step("invalidate the old route and require moved-endpoint progress", async () => {
    await t.fs.writeText(coordination.path("broker-moved"), "moved\n");
    await requireOutput(t, movement, "PASS advertised broker movement");
  });

  await movement.stop();
});

function clusterEnv(first, second, third) {
  return {
    KAFKA_1_HOST_PORT: String(first.port),
    KAFKA_2_HOST_PORT: String(second.port),
    KAFKA_3_HOST_PORT: String(third.port),
  };
}

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

async function requireMetadata(t, probe, root, stack, args, label, expected) {
  let lastObservation = "";
  try {
    await t.poll(
      label,
      async () => {
        const result = await t.cmd(probe, args, { cwd: root, check: false, timeout: "15s" });
        lastObservation = [result.stdout, result.stderr].filter(Boolean).join("\n");
        if (result.exitCode !== 0) {
          throw new Error(lastObservation || `${label} is not ready`);
        }
        expect.value(result.stdout).toContain(expected);
        return result;
      },
      { timeout: "2m", interval: "1s" },
    );
  } catch (error) {
    if (lastObservation) {
      await t.log(`Last ${label} observation:\n${lastObservation}`);
    }
    const logs = await stack.logs({ services: ["kafka-1", "kafka-2", "kafka-3"] }).catch(() => "");
    if (logs) {
      await t.log(`Kafka movement logs:\n${logs}`);
    }
    throw error;
  }
}
