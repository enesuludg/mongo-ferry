# mongo-migrate

Copy documents from a **MongoDB 3.6** source to a **MongoDB 7.x** target.

There is no supported in-place upgrade from 3.6 to 7. This CLI streams a find cursor, batches documents, and writes them with **parallel in-flight** `bulkWrite`s. Existing target `_id`s are replaced by default (`--onConflict replace`).

The Node driver is pinned to **4.17.2** so one process can talk to both 3.6 and 7.x. MongoDB 3.6 is EOL and outside the current driver compatibility matrix; the pin is still the practical choice for a dual-version copy. New 7.x-only wire features are not used.

## Setup

```bash
cp .env.example .env
npm install
```

Fill `SOURCE_URI`, `TARGET_URI`, `SOURCE_DB`, and `TARGET_DB` in `.env`.

## Default: last 30 days of `users`

```bash
npm run migrate -- --collection users
```

Equivalent cursor (range scan on `_id`, no in-memory sort on 3.6):

```js
db.users.find({
  _id: { $gte: ObjectId.createFromTime(Math.floor(Date.now() / 1000) - 30 * 24 * 3600) }
}).sort({ _id: 1 }).hint({ _id: 1 })
```

ObjectId timestamps are second-resolution and only work for ObjectId `_id`s. To filter on a Date field instead:

```bash
npm run migrate -- --collection users --dateField createdAt --since 30d
```

That form can force a 32MB in-memory sort on MongoDB 3.6 if the planner uses the date index while sorting by `_id`. Prefer the default ObjectId window unless you have a matching compound index.

## Cursor-style parameters

```bash
npm run migrate -- \
  --collection users \
  --targetCollection users_v7 \
  --query '{"status":"active"}' \
  --sort '{"_id":1}' \
  --batchSize 1000 \
  --concurrency 16 \
  --sourcePoolSize 16 \
  --targetPoolSize 64 \
  --onConflict replace
```

`--concurrency` is the number of **in-flight bulkWrites**. The cursor only waits for a free slot, then keeps reading.

`--ids` parsing is controlled by `--idType` (`auto` by default). `auto` treats 24-char hex as ObjectId and digit-only values as numbers. If `_id` is actually a string like `"1001"` or a 24-char hex string, pass `--idType string`. Checkpoint and `failed-ids.jsonl` store `_idType` so resume/retry round-trips keep the original type.

```bash
npm run migrate -- --collection users --ids user_abc,1001 --idType string
```

Unknown flags and flags missing a value are errors (`--colection` and `--collection` with no name both fail). `--skip` cannot be combined with `--resume` / `--resumeAfter`.

## Conflicts

| `--onConflict` | Target already has `_id` |
| --- | --- |
| `replace` (default) | Overwrite the whole document |
| `skip` | Leave the target document unchanged |
| `merge` | `$set` source fields; extra target fields stay |

## Throughput

| Flag | Default | Role |
| --- | --- | --- |
| `--batchSize` | `1000` | Documents per `bulkWrite` |
| `--concurrency` | `16` | Parallel in-flight bulk writes |
| `--sourcePoolSize` | `16` | Source connection pool |
| `--targetPoolSize` | `64` | Target connection pool |
| `--writeConcern` | `1` | Fastest durable-enough write; use `majority` if you need it |

Writes use `ordered: false`. Partial `writeErrors` fail only the errored indexes. Permanent errors (auth, validation, duplicate key, `TypeError`) are not retried. Sockets time out after 5 minutes.

## Resume and failed ids

Checkpointing stores the last `_id` of the contiguous batch prefix after both **successful** and **already-recorded failed** seqs. A failed batch is written to `failed-ids.jsonl` and the window can keep moving so a later crash still has a resume point. Recover those `_id`s with `--retryFailed` (successes are then removed from that file). `--resume` / `--resumeAfter` require `sort: {_id:1}` (the default).

```bash
npm run migrate -- --collection users --resume
npm run migrate -- --retryFailed failed-ids.jsonl --collection users
```

`SIGINT` / `SIGTERM` drain in-flight writes, then exit **1** with `migration interrupted` (not `migration finished`). `--stopOnError` also drains, flushes the checkpoint, and closes clients before exiting.

## Dry run and verify

```bash
npm run migrate -- --collection users --dryRun
npm run migrate -- --collection users --verify
npm run migrate -- --collection users --verifyOnly
```

`--dryRun` uses `countDocuments` plus a 5-document sample (honors `--skip` / `--limit`). `--verify` fails only when a **source** `_id` is missing on the target. Extra documents already on the target are expected and do not fail the run.

## Notes

- Target indexes are not copied. Create them on 7.x separately.
- `--projection` always keeps `_id`.
- Use a dedicated migration user with `find` on the source and `insert` / `update` on the target.
