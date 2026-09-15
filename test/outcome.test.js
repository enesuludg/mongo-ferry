import assert from "node:assert/strict";
import test from "node:test";
import { describeRunOutcome, resolveExitCode } from "../src/outcome.js";

test("resolveExitCode is 1 when interrupted or any docs failed", () => {
  assert.equal(resolveExitCode({ interrupted: false, failed: 0 }), 0);
  assert.equal(resolveExitCode({ interrupted: true, failed: 0 }), 1);
  assert.equal(resolveExitCode({ interrupted: false, failed: 10_000 }), 1);
  assert.equal(describeRunOutcome({ failed: 2 }), "migration finished with failures");
  assert.equal(describeRunOutcome({ interrupted: true }), "migration interrupted");
  assert.equal(describeRunOutcome({ failed: 0 }), "migration finished");
});
