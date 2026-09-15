import { ObjectId } from "mongodb";

const OBJECT_ID_HEX = /^[a-fA-F0-9]{24}$/;
const ID_TYPES = new Set(["auto", "objectId", "string", "number"]);

export function assertIdType(idType) {
  if (!ID_TYPES.has(idType)) {
    throw new Error(`Invalid --idType ${idType}. Use auto, objectId, string, or number`);
  }
  return idType;
}

export function describeId(id) {
  if (id && typeof id === "object" && typeof id.toHexString === "function") {
    return { value: id.toHexString(), type: "objectId" };
  }
  if (typeof id === "number") {
    return { value: id, type: "number" };
  }
  return { value: String(id), type: "string" };
}

export function idKey(id) {
  const described = describeId(id);
  return `${described.type}:${described.value}`;
}

export function restoreId(record, fallbackIdType = "auto") {
  if (record && typeof record === "object" && Object.hasOwn(record, "value")) {
    return parseId(record.value, record.type || fallbackIdType);
  }
  return parseId(record, fallbackIdType);
}

export function parseId(value, idType = "auto") {
  if (value instanceof ObjectId || (value && typeof value === "object" && typeof value.toHexString === "function")) {
    if (idType === "string") {
      return value.toHexString();
    }
    return value;
  }
  if (typeof value === "number" && Number.isSafeInteger(value)) {
    if (idType === "string") {
      return String(value);
    }
    if (idType === "objectId") {
      throw new Error(`Invalid ObjectId: ${value}`);
    }
    return value;
  }

  const text = String(value).trim();
  if (idType === "string") {
    return text;
  }
  if (idType === "number") {
    const numeric = Number(text);
    if (!Number.isSafeInteger(numeric) || String(numeric) !== text) {
      throw new Error(`Invalid numeric _id: ${value}`);
    }
    return numeric;
  }
  if (idType === "objectId") {
    if (!OBJECT_ID_HEX.test(text)) {
      throw new Error(`Invalid ObjectId: ${value}`);
    }
    return new ObjectId(text);
  }

  if (OBJECT_ID_HEX.test(text)) {
    return new ObjectId(text);
  }
  if (/^-?\d+$/.test(text)) {
    const numeric = Number(text);
    if (Number.isSafeInteger(numeric) && String(numeric) === text) {
      return numeric;
    }
  }
  return text;
}

export function parseIdList(raw, idType = "auto") {
  if (!raw) {
    return [];
  }

  return String(raw)
    .split(",")
    .map((part) => part.trim())
    .filter(Boolean)
    .map((part) => parseId(part, idType));
}
