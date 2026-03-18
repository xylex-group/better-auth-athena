import {
  createAdapterFactory,
  type AdapterFactory,
  type DBAdapterDebugLogOption,
} from "better-auth/adapters";
import type { BetterAuthOptions } from "better-auth";
import { createClient, type SupabaseClient as AthenaClient } from "@xylex-group/athena";

function toSnakeCase(key: string): string {
  // `userId` -> `user_id`, `createdAt` -> `created_at`
  return key
    .replace(/([a-z0-9])([A-Z])/g, "$1_$2")
    .replace(/__/g, "_")
    .toLowerCase();
}

function toCamelCase(key: string): string {
  // `user_id` -> `userId`, `created_at` -> `createdAt`
  return key.replace(/_([a-z0-9])/g, (_, ch: string) => ch.toUpperCase());
}

function hasUppercase(key: string): boolean {
  return /[A-Z]/.test(key);
}

function mapKeys<T extends Record<string, unknown>>(
  obj: T,
  mapKey: (k: string) => string,
): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(obj)) out[mapKey(k)] = v;
  return out;
}

function mapRowToBetterAuth<T>(row: T): T {
  if (!row || typeof row !== "object") return row;
  if (Array.isArray(row)) return row.map(mapRowToBetterAuth) as unknown as T;
  return mapKeys(row as Record<string, unknown>, toCamelCase) as T;
}

function isLikelyIsoDateString(value: string): boolean {
  // Fast-path: Better Auth commonly uses ISO-8601 timestamps for `*At` fields.
  if (!/^\d{4}-\d{2}-\d{2}T/.test(value)) return false;
  const ms = Date.parse(value);
  return Number.isFinite(ms);
}

function coerceDateFields<T extends Record<string, unknown>>(data: T): T {
  // Athena expects timestamp/timestamptz values as Date (not plain text).
  const out: Record<string, unknown> = { ...data };
  for (const [key, val] of Object.entries(out)) {
    if (val == null) continue;
    if (val instanceof Date) continue;
    if (typeof val === "string" && (key.endsWith("At") || key === "expires") && isLikelyIsoDateString(val)) {
      out[key] = new Date(val);
    }
  }
  return out as T;
}

function toDbRecord<T extends Record<string, unknown>>(data: T): Record<string, unknown> {
  // Better Auth uses camelCase; Athena gateway expects snake_case column names.
  const withDbKeys = mapKeys(data, (k) => (hasUppercase(k) ? toSnakeCase(k) : k));
  return coerceDateFields(withDbKeys);
}

/**
 * Configuration options for the Athena adapter.
 */
export interface AthenaAdapterConfig {
  /**
   * The URL of your Athena gateway.
   */
  url: string;
  /**
   * The API key for authenticating with the Athena gateway.
   */
  apiKey: string;
  /**
   * The client name sent in requests to the Athena gateway.
   */
  client?: string;
  /**
   * Helps you debug issues with the adapter.
   */
  debugLogs?: DBAdapterDebugLogOption;
  /**
   * If the table names in the schema are plural.
   *
   * @default false
   */
  usePlural?: boolean;
}

type AthenaFilterBuilder = {
  eq(col: string, val: unknown): AthenaFilterBuilder;
  neq(col: string, val: unknown): AthenaFilterBuilder;
  gt(col: string, val: unknown): AthenaFilterBuilder;
  gte(col: string, val: unknown): AthenaFilterBuilder;
  lt(col: string, val: unknown): AthenaFilterBuilder;
  lte(col: string, val: unknown): AthenaFilterBuilder;
  in(col: string, vals: unknown[]): AthenaFilterBuilder;
  not(col: string, op?: string, val?: unknown): AthenaFilterBuilder;
  like(col: string, val: string): AthenaFilterBuilder;
};

/**
 * Apply a Better-Auth `CleanedWhere` clause to an Athena filter-chain builder.
 */
function applyWhere<T extends AthenaFilterBuilder>(
  builder: T,
  field: string,
  operator: string,
  value: unknown,
): T {
  const dbField = hasUppercase(field) ? toSnakeCase(field) : field;
  switch (operator) {
    case "eq":
      return builder.eq(dbField, value) as T;
    case "ne":
      return builder.neq(dbField, value) as T;
    case "gt":
      return builder.gt(dbField, value) as T;
    case "gte":
      return builder.gte(dbField, value) as T;
    case "lt":
      return builder.lt(dbField, value) as T;
    case "lte":
      return builder.lte(dbField, value) as T;
    case "in":
      return builder.in(dbField, value as unknown[]) as T;
    case "not_in":
      return builder.not(dbField, "in", value) as T;
    case "contains":
      return builder.like(dbField, `%${value}%`) as T;
    case "starts_with":
      return builder.like(dbField, `${value}%`) as T;
    case "ends_with":
      return builder.like(dbField, `%${value}`) as T;
    default:
      return builder.eq(dbField, value) as T;
  }
}

type WhereClause = { field: string; operator: string; value: unknown };

/**
 * Create a Better-Auth database adapter backed by @xylex-group/athena.
 *
 * Column names are kept in snake_case as required by the Athena gateway.
 *
 * @example
 * ```ts
 * import { betterAuth } from "better-auth";
 * import { athenaAdapter } from "better-auth-athena";
 *
 * export const auth = betterAuth({
 *   database: athenaAdapter({
 *     url: process.env.ATHENA_URL!,
 *     apiKey: process.env.ATHENA_API_KEY!,
 *     client: "my-app",
 *   }),
 * });
 * ```
 */
export const athenaAdapter = (config: AthenaAdapterConfig): AdapterFactory<BetterAuthOptions> => {
  const db: AthenaClient = createClient(config.url, config.apiKey, {
    client: config.client,
  });

  return createAdapterFactory({
    config: {
      adapterId: "athena",
      adapterName: "Athena Adapter",
      usePlural: config.usePlural ?? false,
      debugLogs: config.debugLogs ?? false,
      // Athena/Postgres supports all these natively
      supportsJSON: true,
      supportsDates: true,
      supportsBooleans: true,
      supportsNumericIds: true,
    },
    adapter: () => {
      return {
        // ------------------------------------------------------------------
        // CREATE
        // ------------------------------------------------------------------
        create: async <T extends Record<string, unknown>>({ model, data }: { model: string; data: T; select?: string[] }) => {
          const insertData = toDbRecord(data);
          const { data: result, error } = await db
            .from(model)
            .insert(insertData)
            .select();

          if (error) {
            throw new Error(`[AthenaAdapter] create on "${model}" failed: ${error}`);
          }

          // Athena returns the inserted row(s); take the first one.
          const row = Array.isArray(result) ? result[0] : result;
          return mapRowToBetterAuth((row ?? insertData) as T);
        },

        // ------------------------------------------------------------------
        // UPDATE
        // ------------------------------------------------------------------
        update: async <T>({ model, where, update }: { model: string; where: WhereClause[]; update: T }) => {
          const updateData = toDbRecord(update as Record<string, unknown>);
          let builder = db.from(model).update(updateData);

          for (const clause of where) {
            builder = applyWhere(builder, clause.field, clause.operator, clause.value);
          }

          const { data: result, error } = await builder.select();

          if (error) {
            throw new Error(`[AthenaAdapter] update on "${model}" failed: ${error}`);
          }

          const row = Array.isArray(result) ? result[0] : result;
          return (row ? mapRowToBetterAuth(row as T) : null) as T | null;
        },

        // ------------------------------------------------------------------
        // UPDATE MANY
        // ------------------------------------------------------------------
        updateMany: async ({ model, where, update }: { model: string; where: WhereClause[]; update: Record<string, unknown> }) => {
          const updateData = toDbRecord(update);
          let builder = db.from(model).update(updateData);

          for (const clause of where) {
            builder = applyWhere(builder, clause.field, clause.operator, clause.value);
          }

          const { data: result, error } = await builder.select();

          if (error) {
            throw new Error(`[AthenaAdapter] updateMany on "${model}" failed: ${error}`);
          }

          return Array.isArray(result) ? result.length : (result ? 1 : 0);
        },

        // ------------------------------------------------------------------
        // DELETE
        // ------------------------------------------------------------------
        delete: async ({ model, where }: { model: string; where: WhereClause[] }) => {
          let builder = db.from(model);

          for (const clause of where) {
            builder = applyWhere(builder, clause.field, clause.operator, clause.value);
          }

          const { error } = await builder.delete();

          if (error) {
            throw new Error(`[AthenaAdapter] delete on "${model}" failed: ${error}`);
          }
        },

        // ------------------------------------------------------------------
        // DELETE MANY
        // ------------------------------------------------------------------
        deleteMany: async ({ model, where }: { model: string; where: WhereClause[] }) => {
          let builder = db.from(model);

          for (const clause of where) {
            builder = applyWhere(builder, clause.field, clause.operator, clause.value);
          }

          const { data: result, error } = await builder.delete().select();

          if (error) {
            throw new Error(`[AthenaAdapter] deleteMany on "${model}" failed: ${error}`);
          }

          return Array.isArray(result) ? result.length : (result ? 1 : 0);
        },

        // ------------------------------------------------------------------
        // FIND ONE
        // ------------------------------------------------------------------
        findOne: async <T>({ model, where, select }: { model: string; where: WhereClause[]; select?: string[]; join?: unknown }) => {
          const columns =
            select && select.length > 0
              ? select.map((c) => (hasUppercase(c) ? toSnakeCase(c) : c)).join(", ")
              : undefined;
          let builder = db.from(model).select(columns);

          for (const clause of where) {
            builder = applyWhere(builder, clause.field, clause.operator, clause.value);
          }

          const { data: result, error } = await builder.limit(1);

          if (error) {
            throw new Error(`[AthenaAdapter] findOne on "${model}" failed: ${error}`);
          }

          const rows = Array.isArray(result) ? result : (result ? [result] : []);
          const row = rows[0] ?? null;
          return (row ? mapRowToBetterAuth(row as T) : null) as T | null;
        },

        // ------------------------------------------------------------------
        // FIND MANY
        // ------------------------------------------------------------------
        findMany: async <T>({ model, where, limit, sortBy, offset, select }: {
          model: string;
          where?: WhereClause[];
          limit: number;
          select?: string[];
          sortBy?: { field: string; direction: "asc" | "desc" };
          offset?: number;
          join?: unknown;
        }) => {
          const columns =
            select && select.length > 0
              ? select.map((c) => (hasUppercase(c) ? toSnakeCase(c) : c)).join(", ")
              : undefined;
          let builder = db.from(model).select(columns);

          if (where) {
            for (const clause of where) {
              builder = applyWhere(builder, clause.field, clause.operator, clause.value);
            }
          }

          if (limit !== undefined) {
            builder = builder.limit(limit);
          }

          if (offset !== undefined) {
            builder = builder.offset(offset);
          }

          const { data: result, error } = await builder;

          if (error) {
            throw new Error(`[AthenaAdapter] findMany on "${model}" failed: ${error}`);
          }

          const rows = (Array.isArray(result) ? result : []) as Record<string, unknown>[];
          const betterAuthRows = rows.map((r) => mapRowToBetterAuth(r)) as unknown as T[];

          // The Athena SDK's select chain does not expose a native orderBy/sort
          // method, so we sort the returned rows in memory when sortBy is requested.
          if (sortBy) {
            const sortField = sortBy.field;
            betterAuthRows.sort((a, b) => {
              const aVal = (a as Record<string, unknown>)[sortField];
              const bVal = (b as Record<string, unknown>)[sortField];
              if (aVal == null && bVal == null) return 0;
              if (aVal == null) return sortBy.direction === "asc" ? -1 : 1;
              if (bVal == null) return sortBy.direction === "asc" ? 1 : -1;
              const cmp =
                typeof aVal === "string" && typeof bVal === "string"
                  ? aVal.localeCompare(bVal)
                  : aVal < bVal
                  ? -1
                  : aVal > bVal
                  ? 1
                  : 0;
              return sortBy.direction === "asc" ? cmp : -cmp;
            });
          }

          return betterAuthRows;
        },

        // ------------------------------------------------------------------
        // COUNT
        // ------------------------------------------------------------------
        count: async ({ model, where }: { model: string; where?: WhereClause[] }) => {
          let builder = db.from(model).select();

          if (where) {
            for (const clause of where) {
              builder = applyWhere(builder, clause.field, clause.operator, clause.value);
            }
          }

          const { data: result, error } = await builder;

          if (error) {
            throw new Error(`[AthenaAdapter] count on "${model}" failed: ${error}`);
          }

          return Array.isArray(result) ? result.length : 0;
        },
      };
    },
  });
};

