import assert from "node:assert/strict";
import { ObjectId } from "mongodb";
import test from "node:test";
import { describeId, parseId, parseIdList, restoreId } from "../src/ids.js";

test("parseId auto still hydrates ObjectId hex and integers", () => {
  assert.equal(parseId("user_abc"), "user_abc");
  assert.equal(parseId("1001"), 1001);
  assert.equal(parseId("09"), "09");
  assert.equal(parseId("507f1f77bcf86cd799439011").toHexString(), "507f1f77bcf86cd799439011");
});

test("parseId respects explicit idType so 24-hex and digits stay strings", () => {
  assert.equal(parseId("1001", "string"), "1001");
  assert.equal(parseId("507f1f77bcf86cd799439011", "string"), "507f1f77bcf86cd799439011");
  assert.equal(parseId("1001", "number"), 1001);
});

test("describeId/restoreId round-trip preserves type", () => {
  const objectId = new ObjectId("507f1f77bcf86cd799439011");
  assert.deepEqual(describeId(objectId), { value: "507f1f77bcf86cd799439011", type: "objectId" });
  assert.equal(restoreId({ value: "1001", type: "string" }), "1001");
  assert.equal(restoreId({ value: 1001, type: "number" }), 1001);
  assert.equal(restoreId(describeId(objectId)).toHexString(), objectId.toHexString());
});

test("parseIdList uses idType", () => {
  assert.deepEqual(parseIdList("1001,user_abc", "string"), ["1001", "user_abc"]);
});
