import { expect, smoke } from "smoque";

import { awaitBroker } from "./support/kafka-cluster.mjs";
import { makeBrokerSecretsContainerReadable } from "./support/tls-identities.mjs";

const PROBE = process.platform === "win32" ? "kafka-driver-probe.exe" : "kafka-driver-probe";
const USERNAME = "kafka_driver";
const PASSWORD = "kafka-driver-smoke-secret";
const KEYSTORE_PASSWORD = "kafka-driver-keystore";
const MECHANISMS = ["plain", "scram-sha-256", "scram-sha-512"];

smoke.suite("real Kafka SASL over TLS", { tags: ["real-kafka-security"] }, async (t) => {
  t.redact(PASSWORD);
  t.redact(KEYSTORE_PASSWORD);
  const root = t.repoRoot();
  const composeFile = root.path("smoke", "kafka-security.compose.yml");
  const certificate = root.path("tests", "fixtures", "tls", "localhost-cert.pem");
  const privateKey = root.path("tests", "fixtures", "tls", "localhost-key.pem");
  const probe = root.path("target", "debug", PROBE);
  const secrets = await t.tempDir("kafka-security-secrets");
  const port = await t.ports.reserve("kafka-security");
  const endpoint = `${port.host}:${port.port}`;
  const composeEnv = {
    KAFKA_HOST_PORT: String(port.port),
    KAFKA_SSL_SECRETS: secrets.toString(),
  };
  const docker = await t.tools.docker();

  await t.step("required tools are available", async () => {
    await t.tools.node({ minVersion: "22.18.0" });
    await t.cmd("cargo", ["--version"], { cwd: root, timeout: "10s" });
    await t.cmd("openssl", ["version"], { cwd: root, timeout: "10s" });
    await t.compose.check({ docker: docker.command, cwd: root, timeout: "10s" });
  });

  await t.step("build the public-surface probe", async () => {
    await t.cmd("cargo", ["build", "--locked", "-p", "kafka-driver-probe"], {
      cwd: root,
      timeout: "2m",
    });
    await expect.file(probe).toExist();
  });

  await t.step("prepare an isolated PKCS12 broker identity", async () => {
    await t.cmd(
      "openssl",
      [
        "pkcs12",
        "-export",
        "-in",
        certificate,
        "-inkey",
        privateKey,
        "-out",
        secrets.path("kafka-driver.p12"),
        "-name",
        "kafka-driver-smoke",
        "-passout",
        `pass:${KEYSTORE_PASSWORD}`,
      ],
      { cwd: root, timeout: "10s" },
    );
    await t.fs.writeText(secrets.path("key-password"), KEYSTORE_PASSWORD);
    await t.fs.writeText(secrets.path("store-password"), KEYSTORE_PASSWORD);
    await makeBrokerSecretsContainerReadable(secrets);
  });

  const stack = await t.step("start one SASL_SSL Kafka listener", async () => {
    return await t.compose.up({
      docker: docker.command,
      cwd: root,
      file: composeFile,
      env: composeEnv,
      timeout: "5m",
    });
  });

  await t.step("require the broker to answer an internal health RPC", async () => {
    await awaitBroker(t, docker.command, stack, composeFile, composeEnv, "kafka");
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
    await t.step(`${mechanism} completes over verified TLS`, async () => {
      const result = await t.cmd(
        probe,
        ["tls-authenticate", mechanism, endpoint, certificate, "localhost"],
        {
          cwd: root,
          env: {
            KAFKA_DRIVER_SASL_USERNAME: USERNAME,
            KAFKA_DRIVER_SASL_PASSWORD: PASSWORD,
          },
          timeout: "30s",
        },
      );
      expect.value(result.stdout).toContain(`over rustls certificate verification`);
    });
  }
});
