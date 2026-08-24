import { expect, smoke } from "smoque";

import { awaitBroker } from "./support/kafka-cluster.mjs";
import { makeBrokerSecretsContainerReadable } from "./support/tls-identities.mjs";

const PROBE = process.platform === "win32" ? "kafka-driver-probe.exe" : "kafka-driver-probe";
const KEYSTORE_PASSWORD = "kafka-driver-keystore";

smoke.suite("real Kafka TLS verification", { tags: ["real-kafka-functional", "real-kafka-tls"] }, async (t) => {
  t.redact(KEYSTORE_PASSWORD);
  const root = t.repoRoot();
  const composeFile = root.path("smoke", "kafka-tls.compose.yml");
  const certificate = root.path("tests", "fixtures", "tls", "localhost-cert.pem");
  const privateKey = root.path("tests", "fixtures", "tls", "localhost-key.pem");
  const probe = root.path("target", "debug", PROBE);
  const secrets = await t.tempDir("kafka-tls-secrets");
  const port = await t.ports.reserve("kafka-tls");
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

  const stack = await t.step("start one TLS-protected Kafka listener", async () => {
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

  await t.step("reject a mismatched server identity", async () => {
    const result = await t.cmd(probe, ["tls", endpoint, certificate, "broker.invalid"], {
      cwd: root,
      check: false,
      timeout: "15s",
    });
    if (result.exitCode === 0) {
      t.fail("rustls accepted a certificate for the wrong server identity");
    }
    expect.value(result.stderr).toContain("kafka-driver probe failed");
  });

  await t.step("complete a generated RPC with the verified identity", async () => {
    const result = await t.cmd(probe, ["tls", endpoint, certificate, "localhost"], {
      cwd: root,
      timeout: "30s",
    });
    expect.value(result.stdout).toContain("PASS rustls certificate verification");
  });
});
