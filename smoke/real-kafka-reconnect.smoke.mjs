import { expect, smoke } from "smoque";

const PROBE = process.platform === "win32" ? "kafka-driver-probe.exe" : "kafka-driver-probe";

smoke.suite("real Kafka reconnect", { tags: ["real-kafka-reconnect"] }, async (t) => {
  const root = t.repoRoot();
  const composeFile = root.path("smoke", "kafka.compose.yml");
  const probe = root.path("target", "debug", PROBE);
  const port = await t.ports.reserve("kafka-reconnect");
  const endpoint = `${port.host}:${port.port}`;
  const composeEnv = { KAFKA_HOST_PORT: String(port.port) };
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

  const stack = await t.step("start one pinned Kafka broker", async () => {
    return await t.compose.up({
      docker: docker.command,
      cwd: root,
      file: composeFile,
      env: composeEnv,
      timeout: "5m",
    });
  });

  const reconnect = await t.step("start one long-lived driver session", async () => {
    return await t.process.start(probe, ["reconnect", endpoint], {
      cwd: root,
      name: "kafka-driver-reconnect-probe",
      ready: t.log.contains("READY initial driver connection", { stream: "stdout" }),
      timeout: "30s",
    });
  });

  await t.step("stop Kafka and require observed connection loss", async () => {
    await composeCommand(t, docker.command, stack, composeFile, composeEnv, [
      "stop",
      "--timeout",
      "1",
      "kafka",
    ]);
    await t.poll(
      "existing driver observes broker outage",
      async () => {
        expect.value(reconnect.stdout()).toContain("OBSERVED broker outage");
        return reconnect.stdout();
      },
      { timeout: "30s", interval: "100ms" },
    );
  });

  await t.step("restart Kafka and require same-driver recovery", async () => {
    await composeCommand(t, docker.command, stack, composeFile, composeEnv, ["start", "kafka"]);
    await t.poll(
      "existing driver completes after reconnect",
      async () => {
        if (reconnect.stderr()) {
          throw new Error(reconnect.stderr());
        }
        expect.value(reconnect.stdout()).toContain("PASS existing driver reconnected");
        return reconnect.stdout();
      },
      { timeout: "2m", interval: "250ms" },
    );
  });

  await reconnect.stop();
});

async function composeCommand(t, docker, stack, composeFile, env, args) {
  await t.cmd(
    docker,
    [
      "compose",
      "--project-name",
      stack.projectName,
      "--file",
      composeFile,
      ...args,
    ],
    { cwd: t.repoRoot(), env, timeout: "2m" },
  );
}
