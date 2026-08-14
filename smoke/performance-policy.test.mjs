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

test("one fixed run must prove peak pipeline capability", () => {
  const noisy = {
    ...boundaryReport(),
    sequential_rps: 1200,
    pipelined_rps: 2399,
  };
  const reports = Array.from(
    { length: PERFORMANCE_POLICY.measurementRuns },
    (_, index) => (index === 2 ? boundaryReport() : noisy),
  );

  assert.doesNotThrow(() => assertPerformanceEvidence(reports));
});

test("repeated evidence rejects when no run proves pipeline capability", () => {
  const noisy = {
    ...boundaryReport(),
    sequential_rps: 1200,
    pipelined_rps: 2399,
  };
  const reports = Array.from(
    { length: PERFORMANCE_POLICY.measurementRuns },
    () => noisy,
  );

  assert.throws(
    () => assertPerformanceEvidence(reports),
    /no release performance evidence run proved pipeline capability.*run 1:.*run 5:/,
  );
});

test("every evidence run retains the fixed workload", () => {
  const changed = { ...boundaryReport(), samples: PERFORMANCE_POLICY.samples - 1 };
  const reports = Array.from(
    { length: PERFORMANCE_POLICY.measurementRuns },
    (_, index) => (index === 2 ? changed : boundaryReport()),
  );

  assert.throws(
    () => assertPerformanceEvidence(reports),
    /run 3 changed the workload: samples=/,
  );
});

test("every evidence run retains baseline throughput and control latency", () => {
  for (const [field, value] of [
    ["sequential_rps", PERFORMANCE_POLICY.minimumSequentialRps - 1],
    ["control_under_bulk_ns", PERFORMANCE_POLICY.maximumControlUnderBulkNs + 1],
  ]) {
    const regressed = { ...boundaryReport(), [field]: value };
    const reports = Array.from(
      { length: PERFORMANCE_POLICY.measurementRuns },
      (_, index) => (index === 3 ? regressed : boundaryReport()),
    );

    assert.throws(
      () => assertPerformanceEvidence(reports),
      new RegExp(`run 4 failed a baseline: ${field}=`),
    );
  }
});
