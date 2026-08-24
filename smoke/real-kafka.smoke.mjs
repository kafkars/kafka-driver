import { expect, smoke } from "smoque";

const PROBE = process.platform === "win32" ? "kafka-driver-probe.exe" : "kafka-driver-probe";
const TOPIC = "kafka-driver-smoke";
const GROUP = "kafka-driver-smoke-readers";

smoke.suite("real Kafka cluster routes", { tags: ["real-kafka-functional", "real-kafka"] }, async (t) => {
  const root = t.repoRoot();
  const composeFile = root.path("smoke", "kafka.compose.yml");
  const probe = root.path("target", "debug", PROBE);
  const port = await t.ports.reserve("kafka");
  const endpoint = `${port.host}:${port.port}`;
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
      env: { KAFKA_HOST_PORT: String(port.port) },
      timeout: "5m",
    });
  });

  await t.step("wait for generated ApiVersions readiness", async () => {
    const result = await t.poll(
      "kafka-driver generated ApiVersions round trip",
      async () => {
        const attempt = await t.cmd(probe, ["readiness", endpoint], {
          cwd: root,
          check: false,
          timeout: "10s",
        });
        if (attempt.exitCode !== 0) {
          throw new Error(attempt.stderr || attempt.stdout || "probe exited without diagnostics");
        }
        return attempt;
      },
      { timeout: "2m", interval: "1s" },
    );
    expect.value(result.stdout).toContain("PASS any-broker ApiVersions");
  });

  await t.step("create the exact partition-route topic", async () => {
    await t.cmd(
      docker.command,
      [
        "compose",
        "--project-name",
        stack.projectName,
        "--file",
        composeFile,
        "exec",
        "--no-TTY",
        "kafka",
        "/opt/kafka/bin/kafka-topics.sh",
        "--bootstrap-server",
        "127.0.0.1:19092",
        "--create",
        "--if-not-exists",
        "--topic",
        TOPIC,
        "--partitions",
        "1",
        "--replication-factor",
        "1",
      ],
      { cwd: root, timeout: "30s" },
    );
  });

  await t.step("prove every semantic cluster route", async () => {
    const result = await t.cmd(probe, ["routes", endpoint, TOPIC, GROUP], {
      cwd: root,
      timeout: "30s",
    });
    for (const route of [
      "PASS any-broker route",
      "PASS controller route",
      "PASS group-coordinator route",
      "PASS partition-leader route",
    ]) {
      expect.value(result.stdout).toContain(route);
    }
  });
});
