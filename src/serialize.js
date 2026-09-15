export function serializeForLog(value) {
  return JSON.parse(
    JSON.stringify(value, (_key, current) => {
      if (current instanceof Date) {
        return current.toISOString();
      }
      if (current && typeof current === "object" && typeof current.toHexString === "function") {
        return current.toHexString();
      }
      return current;
    }),
  );
}
