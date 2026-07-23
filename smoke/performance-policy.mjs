// Release qualification thresholds for the fixed real-Kafka workload.

export const PERFORMANCE_POLICY = Object.freeze({
  schema: 1,
  samples: 1000,
  pipelineWidth: 128,
  minimumSequentialRps: 400,
  minimumPipelinedRps: 2000,
  minimumPipelineGain: 2,
  maximumControlUnderBulkNs: 20_000_000,
});

export function assertPerformancePolicy(report) {
  const failures = [];
  exact(failures, "schema", report.schema, PERFORMANCE_POLICY.schema);
  exact(failures, "samples", report.samples, PERFORMANCE_POLICY.samples);
  exact(failures, "pipeline_width", report.pipeline_width, PERFORMANCE_POLICY.pipelineWidth);
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
  if (failures.length !== 0) {
    throw new Error(`release performance policy failed: ${failures.join("; ")}`);
  }
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
