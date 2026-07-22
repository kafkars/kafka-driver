import { expect, smoke } from "smoque";

const PROBE = process.platform === "win32" ? "kafka-driver-probe.exe" : "kafka-driver-probe";
const SAMPLES = 1000;

smoke.suite(
  "real Kafka RPC performance",
  { tags: ["real-kafka-performance"] },
  async (t) => {
    const root = t.repoRoot();
    const composeFile = root.path("smoke", "kafka.compose.yml");
    const probe = root.path("target", "release", PROBE);
    const port = await t.ports.reserve("kafka-performance");
    const endpoint = `${port.host}:${port.port}`;
    const docker = await t.tools.docker();

    await t.step("required tools are available", async () => {
      await t.tools.node({ minVersion: "22.18.0" });
      await t.cmd("cargo", ["--version"], { cwd: root, timeout: "10s" });
      await t.compose.check({ docker: docker.command, cwd: root, timeout: "10s" });
    });

    await t.step("build the release qualification probe", async () => {
      await t.cmd("cargo", ["build", "--locked", "--release", "-p", "kafka-driver-probe"], {
        cwd: root,
        timeout: "5m",
      });
      await expect.file(probe).toExist();
    });

    await t.step("start one pinned Kafka broker", async () => {
      await t.compose.up({
        docker: docker.command,
        cwd: root,
        file: composeFile,
        env: { KAFKA_HOST_PORT: String(port.port) },
        timeout: "5m",
      });
    });

    await t.step("wait for generated ApiVersions readiness", async () => {
      await t.poll(
        "release probe generated ApiVersions round trip",
        async () => {
          const attempt = await t.cmd(probe, ["readiness", endpoint], {
            cwd: root,
            check: false,
            timeout: "10s",
          });
          if (attempt.exitCode !== 0) {
            throw new Error(
              attempt.stderr || attempt.stdout || "probe exited without diagnostics",
            );
          }
          return attempt;
        },
        { timeout: "2m", interval: "1s" },
      );
    });

    await t.step("measure bounded generated RPC progress", async () => {
      const result = await t.cmd(probe, ["measure", endpoint, String(SAMPLES)], {
        cwd: root,
        timeout: "2m",
      });
      await expect.command(result).stdoutJsonPath("$.schema").toBe(1);
      await expect.command(result).stdoutJsonPath("$.samples").toBe(SAMPLES);
      await expect.command(result).stdoutJsonPath("$.pipeline_width").toBe(128);
      await t.log(result.stdout.trim());
      await t.attach.text("real-kafka-performance.json", result.stdout.trim());
    });
  },
);
