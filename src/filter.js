import { ObjectId } from "mongodb";

const DURATION_UNITS = {
  s: 1000,
  m: 60 * 1000,
  h: 60 * 60 * 1000,
  d: 24 * 60 * 60 * 1000,
  w: 7 * 24 * 60 * 60 * 1000,
};

export function parseExtendedJson(raw) {
  if (raw === undefined || raw === null || raw === "") {
    return {};
  }

  const parsed = JSON.parse(raw, (_key, value) => {
    if (!value || typeof value !== "object" || Array.isArray(value)) {
      return value;
    }

    if (typeof value.$oid === "string") {
      return new ObjectId(value.$oid);
    }
    if (value.$date !== undefined) {
      return new Date(value.$date);
    }
    if (typeof value.$numberInt === "string") {
      return Number.parseInt(value.$numberInt, 10);
    }
    if (typeof value.$numberLong === "string") {
      return Number(value.$numberLong);
    }
    if (typeof value.$numberDouble === "string") {
      return Number(value.$numberDouble);
    }

    return value;
  });

  if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) {
    throw new Error("Query must be a JSON object");
  }

  return parsed;
}

export function parseSince(value) {
  if (!value) {
    return new Date(Date.now() - 30 * DURATION_UNITS.d);
  }

  const match = String(value).trim().match(/^(\d+)([smhdw])$/i);
  if (match) {
    const amount = Number(match[1]);
    const unit = DURATION_UNITS[match[2].toLowerCase()];
    return new Date(Date.now() - amount * unit);
  }

  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    throw new Error(`Invalid --since value: ${value}`);
  }
  return date;
}

export function objectIdFromTime(date) {
  return ObjectId.createFromTime(Math.floor(date.getTime() / 1000));
}

export function parseInteger(value, fallback) {
  if (value === undefined || value === null || value === "") {
    return fallback;
  }

  const parsed = Number.parseInt(String(value), 10);
  if (!Number.isInteger(parsed) || parsed < 0) {
    throw new Error(`Expected a non-negative integer, received: ${value}`);
  }
  return parsed;
}

export function parsePositiveInteger(value, fallback) {
  const parsed = parseInteger(value, fallback);
  if (parsed < 1) {
    throw new Error(`Expected an integer >= 1, received: ${value}`);
  }
  return parsed;
}

export function mergeAndFilters(...filters) {
  const clauses = filters.filter((filter) => filter && Object.keys(filter).length > 0);

  if (clauses.length === 0) {
    return {};
  }
  if (clauses.length === 1) {
    return clauses[0];
  }
  return { $and: clauses };
}

export function hasExplicitQuery(query) {
  return Boolean(query && Object.keys(query).length > 0);
}

export function buildCursorFilter({
  query,
  ids,
  dateField,
  since,
  migrateAll,
  resumeAfter,
}) {
  const parts = [query];

  if (ids.length > 0) {
    parts.push({ _id: { $in: ids } });
  } else if (!migrateAll && !hasExplicitQuery(query)) {
    if (dateField) {
      parts.push({ [dateField]: { $gte: since } });
    } else {
      parts.push({ _id: { $gte: objectIdFromTime(since) } });
    }
  }

  if (resumeAfter !== undefined && resumeAfter !== null && resumeAfter !== "") {
    parts.push({ _id: { $gt: resumeAfter } });
  }

  return mergeAndFilters(...parts);
}

export function ensureProjectionKeepsId(projection) {
  if (!projection || Object.keys(projection).length === 0) {
    return undefined;
  }

  return { ...projection, _id: 1 };
}

export function parseSort(raw, { dateField } = {}) {
  if (raw === undefined || raw === null || raw === "") {
    return dateField ? null : { _id: 1 };
  }

  const trimmed = String(raw).trim();
  if (trimmed.toLowerCase() === "none") {
    return null;
  }

  const parsed = parseExtendedJson(trimmed);
  if (Object.keys(parsed).length === 0) {
    return null;
  }
  return parsed;
}

export function hasCursorSort(sort) {
  return Boolean(sort && typeof sort === "object" && Object.keys(sort).length > 0);
}

export function isAscendingIdSort(sort) {
  if (!hasCursorSort(sort)) {
    return false;
  }
  const keys = Object.keys(sort);
  return keys.length === 1 && keys[0] === "_id" && Number(sort._id) === 1;
}
