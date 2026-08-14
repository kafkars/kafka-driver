// Scenarios for Linux-readable broker secrets mounted into Kafka containers.

import assert from "node:assert/strict";
import { mkdtemp, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import { makeBrokerSecretsContainerReadable } from "./support/tls-identities.mjs";

const SECRET_FILES = ["kafka-driver.p12", "key-password", "store-password"];

test(
  "Kafka container users can traverse and read generated broker secrets",
  { skip: process.platform === "win32" },
  async () => {
    const directory = await mkdtemp(join(tmpdir(), "kafka-driver-secrets-"));
    const secrets = {
      path: (name) => join(directory, name),
      toString: () => directory,
    };

    try {
      await Promise.all(
        SECRET_FILES.map((name) => writeFile(secrets.path(name), name, { mode: 0o600 })),
      );
      await makeBrokerSecretsContainerReadable(secrets);

      assert.equal((await stat(directory)).mode & 0o777, 0o755);
      for (const name of SECRET_FILES) {
        assert.equal((await stat(secrets.path(name))).mode & 0o777, 0o644);
      }
    } finally {
      await rm(directory, { recursive: true, force: true });
    }
  },
);
