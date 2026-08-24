import { resolve } from "node:path";

import { expect, smoke } from "smoque";

import {
  awaitBroker,
  startBroker,
  stopBroker,
  upBroker,
} from "./support/kafka-cluster.mjs";
import { createBrokerIdentities } from "./support/tls-identities.mjs";

const KEYSTORE_PASSWORD = "kafka-driver-secure-cluster";
const BROKERS = ["kafka-1", "kafka-2", "kafka-3"];

smoke.suite(
  "real Kafka secure multi-broker recovery",
  { tags: ["real-kafka-functional", "real-kafka-secure-multi-broker"] },
  async (t) => {
    t.redact(KEYSTORE_PASSWORD);
    const root = t.repoRoot();
    const composeFile = root.path("smoke", "kafka-secure-cluster.compose.yml");
    const protocol = resolve(root.toString(), "..", "kafka-protocol");
    const coordination = await t.tempDir("kafka-driver-secure-rolling");
    const probeTarget = await t.tempDir("kafka-driver-secure-target");
    const probeCargoHome = await t.tempDir("kafka-driver-secure-cargo-home");
    const docker = await t.tools.docker();

    await t.step("required tools and sibling protocol are available", async () => {
      await t.tools.node({ minVersion: "22.18.0" });
      await t.cmd("openssl", ["version"], { cwd: root, timeout: "10s" });
      await t.compose.check({ docker: docker.command, cwd: root, timeout: "10s" });
      await expect.file(resolve(protocol, "crates", "kafka-wire", "Cargo.toml")).toExist();
    });

    const tls = await t.step("create one exclusive identity per advertised broker", async () => {
      return await createBrokerIdentities(t, root, BROKERS, KEYSTORE_PASSWORD);
    });
    const composeEnv = {
      KAFKA_1_SSL_SECRETS: tls.identities.get("kafka-1").toString(),
      KAFKA_2_SSL_SECRETS: tls.identities.get("kafka-2").toString(),
      KAFKA_3_SSL_SECRETS: tls.identities.get("kafka-3").toString(),
      KAFKA_DRIVER_SOURCE: root.toString(),
      KAFKA_PROTOCOL_SOURCE: protocol,
      KAFKA_PROBE_TARGET: probeTarget.toString(),
      KAFKA_PROBE_CARGO_HOME: probeCargoHome.toString(),
      KAFKA_COORDINATION: coordination.toString(),
      KAFKA_TLS_AUTHORITY: tls.authority.toString(),
      KAFKA_PROBE_UID: String(process.getuid?.() ?? 0),
      KAFKA_PROBE_GID: String(process.getgid?.() ?? 0),
    };

    const stack = await t.step("start TLS brokers 1 and 3 as a quorum", async () => {
      return await t.compose.up({
        docker: docker.command,
        cwd: root,
        file: composeFile,
        env: composeEnv,
        services: ["kafka-1", "kafka-3"],
        timeout: "6m",
      });
    });

    await t.step("require the initial quorum to answer internal health RPCs", async () => {
      for (const broker of ["kafka-1", "kafka-3"]) {
        await awaitBroker(t, docker.command, stack, composeFile, composeEnv, broker);
      }
    });

    const rolling = await t.step("start one TLS driver on the Compose network", async () => {
      return await t.process.start(
        docker.command,
        [
          "compose",
          "--project-name",
          stack.projectName,
          "--file",
          composeFile,
          "run",
          "--rm",
          "--no-deps",
          "--no-TTY",
          "probe",
          "cargo",
          "run",
          "--locked",
          "--quiet",
          "-p",
          "kafka-driver-probe",
          "--",
          "tls-rolling",
          "kafka-1:9092,kafka-2:9092",
          "/secrets/ca-cert.pem",
          "/coordination",
        ],
        {
          cwd: root,
          env: composeEnv,
          name: "kafka-driver-secure-rolling-probe",
          ready: t.log.contains("READY initial TLS multi-broker connection", {
            stream: "stdout",
          }),
          timeout: "8m",
        },
      );
    });

    await t.step("bring broker 2 online with its own certificate identity", async () => {
      await upBroker(t, docker.command, stack, composeFile, composeEnv, "kafka-2");
      await awaitBroker(t, docker.command, stack, composeFile, composeEnv, "kafka-2");
    });

    await t.step("lose broker 1 and recover through broker 2 SNI", async () => {
      await stopBroker(t, docker.command, stack, composeFile, composeEnv, "kafka-1");
      await t.fs.writeText(coordination.path("broker-1-stopped"), "stopped\n");
      await requireOutput(t, rolling, "RECOVERED TLS broker failover 1");
    });

    await t.step("restore broker 1 before the second identity rotation", async () => {
      await startBroker(t, docker.command, stack, composeFile, composeEnv, "kafka-1");
      await awaitBroker(t, docker.command, stack, composeFile, composeEnv, "kafka-1");
    });

    await t.step("lose broker 2 and recover through broker 1 SNI", async () => {
      await stopBroker(t, docker.command, stack, composeFile, composeEnv, "kafka-2");
      await t.fs.writeText(coordination.path("broker-2-stopped"), "stopped\n");
      await requireOutput(t, rolling, "PASS TLS broker failover 2");
    });

    await t.step("restore broker 2", async () => {
      await startBroker(t, docker.command, stack, composeFile, composeEnv, "kafka-2");
      await awaitBroker(t, docker.command, stack, composeFile, composeEnv, "kafka-2");
    });

    await rolling.stop();
  },
);

async function requireOutput(t, process, expected) {
  await t.poll(
    expected,
    async () => {
      if (process.stderr().includes("kafka-driver probe failed")) {
        throw new Error(process.stderr());
      }
      expect.value(process.stdout()).toContain(expected);
      return process.stdout();
    },
    { timeout: "2m", interval: "250ms" },
  );
}
