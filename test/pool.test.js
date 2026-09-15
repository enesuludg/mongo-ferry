import assert from "node:assert/strict";
import test from "node:test";
import { createTaskPool } from "../src/pool.js";

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

test("schedule waits for a free slot, not for the bulkWrite to finish", async () => {
  const pool = createTaskPool(8);
  let current = 0;
  let peak = 0;
  const started = Date.now();

  for (let index = 0; index < 10; index += 1) {
    await pool.schedule(async () => {
      current += 1;
      peak = Math.max(peak, current);
      await sleep(50);
      current -= 1;
    });
  }

  await pool.drain();
  const wallClockMs = Date.now() - started;

  assert.equal(peak, 8);
  assert.ok(wallClockMs < 250, `expected parallel wall clock, got ${wallClockMs}ms`);
});

test("createTaskPool caps concurrency and drain swallows rejected tasks", async () => {
  const pool = createTaskPool(2);
  let peak = 0;
  let active = 0;
  let failures = 0;

  const work = async (shouldFail) => {
    active += 1;
    peak = Math.max(peak, active);
    await sleep(10);
    active -= 1;
    if (shouldFail) {
      failures += 1;
      throw new Error("boom");
    }
  };

  await pool.schedule(() => work(false));
  await pool.schedule(() => work(true));
  await pool.schedule(() => work(false));
  await pool.drain();

  assert.ok(peak <= 2);
  assert.equal(failures, 1);
});

test("drain waits for fire-and-forget schedule calls", async () => {
  const pool = createTaskPool(2);
  let finished = 0;

  for (let index = 0; index < 5; index += 1) {
    pool.schedule(async () => {
      await sleep(20);
      finished += 1;
    });
  }

  await pool.drain();
  assert.equal(finished, 5);
});
