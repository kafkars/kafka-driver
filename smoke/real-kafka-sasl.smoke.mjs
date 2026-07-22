import { expect, smoke } from "smoque";

const PROBE = process.platform === "win32" ? "kafka-driver-probe.exe" : "kafka-driver-probe";
const USERNAME = "kafka_driver";
const PASSWORD = "kafka-driver-smoke-secret";
const MECHANISMS = ["plain", "scram-sha-256", "scram-sha-512"];

smoke.suite("real Kafka SASL authentication", { tags: ["real-kafka-sasl"] }, async (t) => {
  t.redact(PASSWORD);
  const root = t.repoRoot();
  const composeFile = root.path("smoke", "kafka-sasl.compose.yml");
  const probe = root.path("target", "debug", PROBE);
  const port = await t.ports.reserve("kafka-sasl");
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

  const stack = await t.step("start one SASL-protected Kafka listener", async () => {
    return await t.compose.up({
      docker: docker.command,
      cwd: root,
      file: composeFile,
      env: { KAFKA_HOST_PORT: String(port.port) },
      timeout: "5m",
    });
  });

  await t.step("install bounded SCRAM credentials", async () => {
    for (const mechanism of ["SCRAM-SHA-256", "SCRAM-SHA-512"]) {
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
          "/opt/kafka/bin/kafka-configs.sh",
          "--bootstrap-server",
          "127.0.0.1:19092",
          "--alter",
          "--add-config",
          `${mechanism}=[iterations=4096,password=${PASSWORD}]`,
          "--entity-type",
          "users",
          "--entity-name",
          USERNAME,
        ],
        { cwd: root, timeout: "30s" },
      );
    }
  });

  for (const mechanism of MECHANISMS) {
    await t.step(`authenticate with ${mechanism}`, async () => {
      const result = await t.cmd(probe, ["authenticate", mechanism, endpoint], {
        cwd: root,
        env: {
          KAFKA_DRIVER_SASL_USERNAME: USERNAME,
          KAFKA_DRIVER_SASL_PASSWORD: PASSWORD,
        },
        timeout: "30s",
      });
      expect.value(result.stdout).toContain(`PASS ${label(mechanism)} authentication`);
    });
  }
});

function label(mechanism) {
  switch (mechanism) {
    case "plain":
      return "SASL PLAIN";
    case "scram-sha-256":
      return "SCRAM-SHA-256";
    case "scram-sha-512":
      return "SCRAM-SHA-512";
    default:
      throw new Error(`unsupported SASL mechanism: ${mechanism}`);
  }
}
