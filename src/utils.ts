/**
 * Shared helpers and types for the Athena adapter.
 */

export function toSnakeCase(key: string): string {
  return key
    .replace(/([a-z0-9])([A-Z])/g, "$1_$2")
    .replace(/__/g, "_")
    .toLowerCase();
}

export function toCamelCase(key: string): string {
  return key.replace(/_([a-z0-9])/g, (_, ch: string) => ch.toUpperCase());
}

export function hasUppercase(key: string): boolean {
  return /[A-Z]/.test(key);
}

export function mapKeys<T extends Record<string, unknown>>(
  obj: T,
  mapKey: (k: string) => string,
): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(obj)) out[mapKey(k)] = v;
  return out;
}

export function mapRowToBetterAuth<T>(row: T): T {
  if (!row || typeof row !== "object") return row;
  if (Array.isArray(row)) return row.map(mapRowToBetterAuth) as unknown as T;
  return mapKeys(row as Record<string, unknown>, toCamelCase) as T;
}

export function isLikelyIsoDateString(value: string): boolean {
  if (!/^\d{4}-\d{2}-\d{2}T/.test(value)) return false;
  const ms = Date.parse(value);
  return Number.isFinite(ms);
}

export function isTimestampKey(key: string): boolean {
  return key.endsWith("At") || key.endsWith("_at") || key === "expires";
}

export function coerceDateFields<T extends Record<string, unknown>>(data: T): T {
  const out: Record<string, unknown> = { ...data };
  for (const [key, val] of Object.entries(out)) {
    if (val == null) continue;
    if (
      typeof val === "string" &&
      isTimestampKey(key) &&
      isLikelyIsoDateString(val)
    ) {
      out[key] = new Date(val);
    }
  }
  return out as T;
}

export function toDbRecord<T extends Record<string, unknown>>(
  data: T,
): Record<string, unknown> {
  const withDbKeys = mapKeys(data, (k) =>
    hasUppercase(k) ? toSnakeCase(k) : k,
  );
  return coerceDateFields(withDbKeys);
}

export type AthenaFilterBuilder = {
  eq(col: string, val: unknown): AthenaFilterBuilder;
  neq(col: string, val: unknown): AthenaFilterBuilder;
  gt(col: string, val: unknown): AthenaFilterBuilder;
  gte(col: string, val: unknown): AthenaFilterBuilder;
  lt(col: string, val: unknown): AthenaFilterBuilder;
  lte(col: string, val: unknown): AthenaFilterBuilder;
  in(col: string, vals: unknown[]): AthenaFilterBuilder;
  not(col: string, op?: string, val?: unknown): AthenaFilterBuilder;
  like(col: string, val: string): AthenaFilterBuilder;
  delete?(): { select(): Promise<{ data: unknown; error: unknown }> };
  select?(columns?: string): Promise<{ data: unknown; error: unknown }> | { select(): Promise<{ data: unknown; error: unknown }> };
};

export type WhereClause = { field: string; operator: string; value: unknown };

const defaultColumnMapper = (col: string) =>
  hasUppercase(col) ? toSnakeCase(col) : col;

export function applyWhere(
  builder: AthenaFilterBuilder,
  field: string,
  operator: string,
  value: unknown,
  columnMapper: (col: string) => string = defaultColumnMapper,
): AthenaFilterBuilder {
  const dbField = columnMapper(field);
  switch (operator) {
    case "eq":
      return builder.eq(dbField, value);
    case "ne":
      return builder.neq(dbField, value);
    case "gt":
      return builder.gt(dbField, value);
    case "gte":
      return builder.gte(dbField, value);
    case "lt":
      return builder.lt(dbField, value);
    case "lte":
      return builder.lte(dbField, value);
    case "in":
      return builder.in(dbField, value as unknown[]);
    case "not_in":
      return builder.not(dbField, "in", value);
    case "contains":
      return builder.like(dbField, `%${value}%`);
    case "starts_with":
      return builder.like(dbField, `${value}%`);
    case "ends_with":
      return builder.like(dbField, `%${value}`);
    default:
      return builder.eq(dbField, value);
  }
}

export function isMissingColumnError(error: unknown): boolean {
  const msg = String(error ?? "");
  return (
    msg.includes("specified column does not exist") ||
    msg.includes("column does not exist")
  );
}

/** True when the Athena gateway returns a success message in the error field (treat as success). */
export function isSuccessMessageInError(error: unknown): boolean {
  const msg = String(error ?? "").toLowerCase();
  return (
    msg === "data inserted successfully" ||
    msg === "data updated successfully" ||
    msg === "data deleted successfully"
  );
}

export function snakeMapper(col: string): string {
  return hasUppercase(col) ? toSnakeCase(col) : col;
}

export function identityMapper(col: string): string {
  return col;
}
