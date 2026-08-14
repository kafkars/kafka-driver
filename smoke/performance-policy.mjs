// Release qualification thresholds for the fixed real-Kafka workload.

export const PERFORMANCE_POLICY = Object.freeze({
  schema: 1,
  samples: 1000,
  pipelineWidth: 128,
  measurementRuns: 3,
  minimumPassingRuns: 2,
  minimumSequentialRps: 400,
  minimumPipelinedRps: 2000,
  minimumPipelineGain: 2,
  maximumControlUnderBulkNs: 20_000_000,
});

export function assertPerformancePolicy(report) {
  const failures = workloadFailures(report);
  failures.push(...measurementFailures(report));
  if (failures.length !== 0) {
    throw new Error(`release performance policy failed: ${failures.join("; ")}`);
  }
}

export function assertPerformanceEvidence(reports) {
  if (reports.length !== PERFORMANCE_POLICY.measurementRuns) {
    throw new Error(
      `release performance evidence has ${reports.length} runs; expected ` +
        PERFORMANCE_POLICY.measurementRuns,
    );
  }

  for (const [index, report] of reports.entries()) {
    const failures = workloadFailures(report);
    if (failures.length !== 0) {
      throw new Error(
        `release performance evidence run ${index + 1} changed the workload: ` +
          failures.join("; "),
      );
    }
  }

  const failedRuns = reports
    .map((report, index) => ({ index, failures: measurementFailures(report) }))
    .filter(({ failures }) => failures.length !== 0);
  const passingRuns = reports.length - failedRuns.length;
  if (passingRuns < PERFORMANCE_POLICY.minimumPassingRuns) {
    const details = failedRuns
      .map(({ index, failures }) => `run ${index + 1}: ${failures.join("; ")}`)
      .join(" | ");
    throw new Error(
      `release performance evidence has ${passingRuns} passing runs; expected at least ` +
        `${PERFORMANCE_POLICY.minimumPassingRuns}; ${details}`,
    );
  }
}

function workloadFailures(report) {
  const failures = [];
  exact(failures, "schema", report.schema, PERFORMANCE_POLICY.schema);
  exact(failures, "samples", report.samples, PERFORMANCE_POLICY.samples);
  exact(failures, "pipeline_width", report.pipeline_width, PERFORMANCE_POLICY.pipelineWidth);
  return failures;
}

function measurementFailures(report) {
  const failures = [];
  minimum(
    failures,
    "sequential_rps",
    report.sequential_rps,
    PERFORMANCE_POLICY.minimumSequentialRps,
  );
  minimum(
    failures,
    "pipelined_rps",
    report.pipelined_rps,
    PERFORMANCE_POLICY.minimumPipelinedRps,
  );
  maximum(
    failures,
    "control_under_bulk_ns",
    report.control_under_bulk_ns,
    PERFORMANCE_POLICY.maximumControlUnderBulkNs,
  );
  minimum(
    failures,
    "pipeline gain",
    report.pipelined_rps,
    report.sequential_rps * PERFORMANCE_POLICY.minimumPipelineGain,
  );
  return failures;
}

function exact(failures, name, actual, expected) {
  if (actual !== expected) {
    failures.push(`${name}=${actual}; expected ${expected}`);
  }
}

function minimum(failures, name, actual, expected) {
  if (!Number.isSafeInteger(actual) || actual < expected) {
    failures.push(`${name}=${actual}; expected at least ${expected}`);
  }
}

function maximum(failures, name, actual, expected) {
  if (!Number.isSafeInteger(actual) || actual > expected) {
    failures.push(`${name}=${actual}; expected at most ${expected}`);
  }
}
