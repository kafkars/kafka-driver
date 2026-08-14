// Scenarios for the fixed real-Kafka release performance policy.

import assert from "node:assert/strict";
import test from "node:test";

import {
  PERFORMANCE_POLICY,
  assertPerformanceEvidence,
  assertPerformancePolicy,
} from "./performance-policy.mjs";

function boundaryReport() {
  return {
    schema: PERFORMANCE_POLICY.schema,
    samples: PERFORMANCE_POLICY.samples,
    pipeline_width: PERFORMANCE_POLICY.pipelineWidth,
    sequential_rps: PERFORMANCE_POLICY.minimumSequentialRps,
    pipelined_rps: PERFORMANCE_POLICY.minimumPipelinedRps,
    control_under_bulk_ns: PERFORMANCE_POLICY.maximumControlUnderBulkNs,
  };
}

test("exact release boundaries are accepted", () => {
  assert.doesNotThrow(() => assertPerformancePolicy(boundaryReport()));
});

test("each throughput and latency regression is named", () => {
  for (const [field, value] of [
    ["sequential_rps", PERFORMANCE_POLICY.minimumSequentialRps - 1],
    ["pipelined_rps", PERFORMANCE_POLICY.minimumPipelinedRps - 1],
    ["control_under_bulk_ns", PERFORMANCE_POLICY.maximumControlUnderBulkNs + 1],
  ]) {
    assert.throws(
      () => assertPerformancePolicy({ ...boundaryReport(), [field]: value }),
      new RegExp(field),
    );
  }
});

test("pipelining must retain its minimum gain over sequential calls", () => {
  const report = {
    ...boundaryReport(),
    sequential_rps: 1200,
    pipelined_rps: 2399,
  };

  assert.throws(() => assertPerformancePolicy(report), /pipeline gain/);
});

test("repeated evidence tolerates one runner-noise regression", () => {
  const noisy = {
    ...boundaryReport(),
    sequential_rps: 1200,
    pipelined_rps: 2399,
  };

  assert.doesNotThrow(() =>
    assertPerformanceEvidence([boundaryReport(), noisy, boundaryReport()]),
  );
});

test("repeated evidence rejects two performance regressions", () => {
  const noisy = {
    ...boundaryReport(),
    sequential_rps: 1200,
    pipelined_rps: 2399,
  };

  assert.throws(
    () => assertPerformanceEvidence([noisy, boundaryReport(), noisy]),
    /1 passing runs; expected at least 2.*run 1:.*run 3:/,
  );
});

test("every evidence run retains the fixed workload", () => {
  const changed = { ...boundaryReport(), samples: PERFORMANCE_POLICY.samples - 1 };

  assert.throws(
    () => assertPerformanceEvidence([boundaryReport(), boundaryReport(), changed]),
    /run 3 changed the workload: samples=/,
  );
});
