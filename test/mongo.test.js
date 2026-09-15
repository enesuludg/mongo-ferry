import assert from "node:assert/strict";
import test from "node:test";
import { parseWriteConcern } from "../src/mongo.js";

test("parseWriteConcern defaults to w:1", () => {
  assert.deepEqual(parseWriteConcern(undefined), { w: 1 });
  assert.deepEqual(parseWriteConcern("1"), { w: 1 });
  assert.deepEqual(parseWriteConcern("majority"), { w: "majority" });
  assert.deepEqual(parseWriteConcern("2"), { w: 2 });
});
