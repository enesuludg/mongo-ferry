import assert from "node:assert/strict";
import test from "node:test";
import { parseArgs } from "../src/cli.js";

test("parseArgs maps flags and boolean switches", () => {
  const args = parseArgs([
    "--collection",
    "users",
    "--dryRun",
    "--batchSize",
    "500",
    "--resume",
  ]);

  assert.equal(args.collection, "users");
  assert.equal(args.batchSize, "500");
  assert.equal(args.dryRun, true);
  assert.equal(args.resume, true);
});

test("parseArgs rejects unknown flags", () => {
  assert.throws(() => parseArgs(["--colection", "orders"]), /Unknown flag --colection/);
});

test("parseArgs rejects value flags without a value", () => {
  assert.throws(() => parseArgs(["--collection"]), /Flag --collection requires a value/);
  assert.throws(() => parseArgs(["--collection", "--batchSize", "10"]), /Flag --collection requires a value/);
});
