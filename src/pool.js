export function createTaskPool(concurrency) {
  if (!Number.isInteger(concurrency) || concurrency < 1) {
    throw new Error(`concurrency must be >= 1, received: ${concurrency}`);
  }

  let active = 0;
  let submitted = 0;
  let completed = 0;
  const waiters = [];
  const inFlight = new Set();
  const drainWaiters = [];

  async function acquire() {
    if (active < concurrency) {
      active += 1;
      return;
    }
    await new Promise((resolve) => {
      waiters.push(resolve);
    });
  }

  function release() {
    if (waiters.length > 0) {
      waiters.shift()();
      return;
    }
    active -= 1;
  }

  function notifyDrain() {
    if (completed === submitted) {
      for (const resolve of drainWaiters.splice(0)) {
        resolve();
      }
    }
  }

  async function schedule(task) {
    submitted += 1;
    await acquire();
    const running = Promise.resolve()
      .then(task)
      .catch(() => undefined)
      .finally(() => {
        inFlight.delete(running);
        release();
        completed += 1;
        notifyDrain();
      });
    inFlight.add(running);
  }

  async function drain() {
    if (completed === submitted) {
      return;
    }
    await new Promise((resolve) => {
      drainWaiters.push(resolve);
    });
  }

  return {
    schedule,
    drain,
    size: () => inFlight.size,
    active: () => active,
  };
}
