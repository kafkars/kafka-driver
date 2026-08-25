// Managed three-node Kafka Compose operations shared by cluster qualification.

export async function awaitBroker(t, docker, stack, composeFile, env, broker) {
  try {
    await t.poll(
      `${broker} accepts an internal Kafka RPC`,
      async () => {
        const result = await composeCommand(t, docker, stack, composeFile, env, [
          "exec",
          "--no-TTY",
          broker,
          "/opt/kafka/bin/kafka-broker-api-versions.sh",
          "--bootstrap-server",
          "127.0.0.1:19092",
        ], false);
        if (result.exitCode !== 0) {
          throw new Error(result.stderr || result.stdout || `${broker} is not ready`);
        }
        return result;
      },
      { timeout: "2m", interval: "1s" },
    );
  } catch (error) {
    const logs = await stack.logs({ services: [broker] }).catch(() => "");
    if (logs) {
      await t.log(`${broker} startup logs:\n${logs}`);
    }
    throw error;
  }
}

export async function stopBroker(t, docker, stack, composeFile, env, broker, timeout = "1") {
  await composeCommand(t, docker, stack, composeFile, env, ["stop", "--timeout", timeout, broker]);
}

export async function startBroker(t, docker, stack, composeFile, env, broker) {
  await composeCommand(t, docker, stack, composeFile, env, ["start", broker]);
}

export async function upBroker(t, docker, stack, composeFile, env, broker, options = []) {
  await composeCommand(t, docker, stack, composeFile, env, [
    "up",
    "--detach",
    ...options,
    broker,
  ]);
}

export async function composeCommand(
  t,
  docker,
  stack,
  composeFile,
  env,
  args,
  check = true,
) {
  return await t.cmd(
    docker,
    ["compose", "--project-name", stack.projectName, "--file", composeFile, ...args],
    { cwd: t.repoRoot(), env, check, timeout: "2m" },
  );
}
