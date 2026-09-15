import { MongoClient } from "mongodb";

export function parseWriteConcern(value) {
  if (value === undefined || value === "" || value === "1") {
    return { w: 1 };
  }
  if (value === "majority") {
    return { w: "majority" };
  }

  const numeric = Number.parseInt(String(value), 10);
  if (Number.isInteger(numeric) && numeric >= 0) {
    return { w: numeric };
  }

  return { w: String(value) };
}

function databaseFromUri(uri) {
  try {
    const normalized = uri.replace(/^mongodb(\+srv)?:\/\//, "http://");
    const pathname = new URL(normalized).pathname.replace(/^\//, "");
    const name = pathname.split("/")[0];
    return name || undefined;
  } catch {
    return undefined;
  }
}

export function resolveDatabaseName(explicitName, uri) {
  return explicitName || databaseFromUri(uri);
}

const SOCKET_TIMEOUT_MS = 300_000;

export function createSourceClient(uri, poolSize, directConnection = false) {
  return new MongoClient(uri, {
    maxPoolSize: poolSize,
    minPoolSize: Math.min(4, poolSize),
    retryWrites: false,
    readPreference: "secondaryPreferred",
    connectTimeoutMS: 30_000,
    serverSelectionTimeoutMS: 30_000,
    socketTimeoutMS: SOCKET_TIMEOUT_MS,
    ...(directConnection ? { directConnection: true } : {}),
  });
}

export function createTargetClient(uri, poolSize, writeConcern, directConnection = false) {
  return new MongoClient(uri, {
    maxPoolSize: poolSize,
    minPoolSize: Math.min(8, poolSize),
    retryWrites: true,
    w: parseWriteConcern(writeConcern).w,
    connectTimeoutMS: 30_000,
    serverSelectionTimeoutMS: 30_000,
    socketTimeoutMS: SOCKET_TIMEOUT_MS,
    ...(directConnection ? { directConnection: true } : {}),
  });
}

export async function pingClient(client, label) {
  await client.db("admin").command({ ping: 1 });
  return label;
}
