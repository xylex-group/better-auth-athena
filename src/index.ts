import {
  createAdapterFactory,
  type AdapterFactory,
  type DBAdapterDebugLogOption,
} from "better-auth/adapters";
import type { BetterAuthOptions } from "better-auth";
import { createClient, type SupabaseClient as AthenaClient } from "@xylex-group/athena";

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
  switch (operator) {
    case "eq":
      return builder.eq(field, value) as T;
    case "ne":
      return builder.neq(field, value) as T;
    case "gt":
      return builder.gt(field, value) as T;
    case "gte":
      return builder.gte(field, value) as T;
    case "lt":
      return builder.lt(field, value) as T;
    case "lte":
      return builder.lte(field, value) as T;
    case "in":
      return builder.in(field, value as unknown[]) as T;
    case "not_in":
      return builder.not(field, "in", value) as T;
    case "contains":
      return builder.like(field, `%${value}%`) as T;
    case "starts_with":
      return builder.like(field, `${value}%`) as T;
    case "ends_with":
      return builder.like(field, `%${value}`) as T;
    default:
      return builder.eq(field, value) as T;
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
          const { data: result, error } = await db
            .from(model)
            .insert(data)
            .select();

          if (error) {
            throw new Error(`[AthenaAdapter] create on "${model}" failed: ${error}`);
          }

          // Athena returns the inserted row(s); take the first one.
          const row = Array.isArray(result) ? result[0] : result;
          return (row ?? data) as T;
        },

        // ------------------------------------------------------------------
        // UPDATE
        // ------------------------------------------------------------------
        update: async <T>({ model, where, update }: { model: string; where: WhereClause[]; update: T }) => {
          let builder = db.from(model).update(update as Record<string, unknown>);

          for (const clause of where) {
            builder = applyWhere(builder, clause.field, clause.operator, clause.value);
          }

          const { data: result, error } = await builder.select();

          if (error) {
            throw new Error(`[AthenaAdapter] update on "${model}" failed: ${error}`);
          }

          const row = Array.isArray(result) ? result[0] : result;
          return (row ?? null) as T | null;
        },

        // ------------------------------------------------------------------
        // UPDATE MANY
        // ------------------------------------------------------------------
        updateMany: async ({ model, where, update }: { model: string; where: WhereClause[]; update: Record<string, unknown> }) => {
          let builder = db.from(model).update(update);

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
          const columns = select && select.length > 0 ? select.join(", ") : undefined;
          let builder = db.from(model).select(columns);

          for (const clause of where) {
            builder = applyWhere(builder, clause.field, clause.operator, clause.value);
          }

          const { data: result, error } = await builder.limit(1);

          if (error) {
            throw new Error(`[AthenaAdapter] findOne on "${model}" failed: ${error}`);
          }

          const rows = Array.isArray(result) ? result : (result ? [result] : []);
          return (rows[0] ?? null) as T | null;
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
          const columns = select && select.length > 0 ? select.join(", ") : undefined;
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

          // The Athena SDK's select chain does not expose a native orderBy/sort
          // method, so we sort the returned rows in memory when sortBy is requested.
          if (sortBy) {
            rows.sort((a, b) => {
              const aVal = a[sortBy.field];
              const bVal = b[sortBy.field];
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

          return rows as T[];
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

