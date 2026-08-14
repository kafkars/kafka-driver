// Release qualification thresholds for the fixed real-Kafka workload.

export const PERFORMANCE_POLICY = Object.freeze({
  schema: 1,
  samples: 1000,
  pipelineWidth: 128,
  measurementRuns: 5,
  minimumSequentialRps: 400,
  minimumPipelinedRps: 2000,
  minimumPipelineGain: 2,
  maximumControlUnderBulkNs: 20_000_000,
});

export function assertPerformancePolicy(report) {
  const failures = workloadFailures(report);
  failures.push(...baselineFailures(report), ...pipelineFailures(report));
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

  for (const [index, report] of reports.entries()) {
    const failures = baselineFailures(report);
    if (failures.length !== 0) {
      throw new Error(
        `release performance evidence run ${index + 1} failed a baseline: ` +
          failures.join("; "),
      );
    }
  }

  const pipelineAttempts = reports.map((report, index) => ({
    index,
    failures: pipelineFailures(report),
  }));
  if (pipelineAttempts.every(({ failures }) => failures.length !== 0)) {
    const details = pipelineAttempts
      .map(({ index, failures }) => `run ${index + 1}: ${failures.join("; ")}`)
      .join(" | ");
    throw new Error(
      `no release performance evidence run proved pipeline capability; ${details}`,
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

function baselineFailures(report) {
  const failures = [];
  minimum(
    failures,
    "sequential_rps",
    report.sequential_rps,
    PERFORMANCE_POLICY.minimumSequentialRps,
  );
  maximum(
    failures,
    "control_under_bulk_ns",
    report.control_under_bulk_ns,
    PERFORMANCE_POLICY.maximumControlUnderBulkNs,
  );
  return failures;
}

function pipelineFailures(report) {
  const failures = [];
  minimum(
    failures,
    "pipelined_rps",
    report.pipelined_rps,
    PERFORMANCE_POLICY.minimumPipelinedRps,
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
