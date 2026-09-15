import assert from "node:assert/strict";
import test from "node:test";
import { describeRunOutcome, formatDuration, resolveExitCode } from "../src/outcome.js";

test("resolveExitCode is 1 when interrupted or any docs failed", () => {
  assert.equal(resolveExitCode({ interrupted: false, failed: 0 }), 0);
  assert.equal(resolveExitCode({ interrupted: true, failed: 0 }), 1);
  assert.equal(resolveExitCode({ interrupted: false, failed: 10_000 }), 1);
  assert.equal(describeRunOutcome({ failed: 2 }), "migration finished with failures");
  assert.equal(describeRunOutcome({ interrupted: true }), "migration interrupted");
  assert.equal(describeRunOutcome({ failed: 0 }), "migration finished");
  assert.equal(describeRunOutcome({ failed: 0, elapsedMs: 318_000 }), "migration finished in 5m 18s");
  assert.equal(describeRunOutcome({ interrupted: true, elapsedMs: 12_000 }), "migration interrupted in 12s");
  assert.equal(
    describeRunOutcome({ failed: 0, elapsedMs: 12_000, created: 80, updated: 20 }),
    "migration finished in 12s: created 80, updated 20",
  );
});

test("formatDuration renders a compact timer", () => {
  assert.equal(formatDuration(0), "0ms");
  assert.equal(formatDuration(850), "850ms");
  assert.equal(formatDuration(12_000), "12s");
  assert.equal(formatDuration(318_000), "5m 18s");
  assert.equal(formatDuration(3_725_000), "1h 2m 5s");
});
