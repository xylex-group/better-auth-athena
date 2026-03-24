import type { WhereClause, AthenaFilterBuilder } from "../utils";
import {
  toDbRecord,
  mapRowToBetterAuth,
  applyWhere,
  isSuccessMessageInError,
  toSnakeCase,
  hasUppercase,
} from "../utils";

export type GatewayConnectionConfig = {
  url: string;
  apiKey: string;
  client?: string;
  headers?: Record<string, string>;
};

export type UpdateDeps = {
  ensureDbClient: () => any;
  getConnectionConfig: () => GatewayConnectionConfig;
};

export function updateMethod(deps: UpdateDeps) {
  const { ensureDbClient } = deps;

  return async function update<T>({
    model,
    where,
    update,
  }: {
    model: string;
    where: WhereClause[];
    update: T;
  }) {
    const db = ensureDbClient();
    const updateData = toDbRecord(update as Record<string, unknown>);
    const debugUpdates = process?.env?.ATHENA_ADAPTER_DEBUG_UPDATES === "1";
    const debugLog = (event: string, extra?: Record<string, unknown>) => {
      if (!debugUpdates) return;
      console.info("[AthenaAdapter][update]", { event, model, ...extra });
    };

    // Build an Athena filter-builder chain with one of two update_body shapes.
    const build = (shape: "plain" | "wrapped") => {
      const values =
        shape === "plain"
          ? (updateData as any)
          : ({ data: updateData, set: updateData } as any);
      let b = db.from(model).update(values) as AthenaFilterBuilder;
      for (const clause of where) {
        b = applyWhere(b, clause.field, clause.operator, clause.value);
      }
      return b;
    };

    const run = async (b: AthenaFilterBuilder) => {
      const { data: result, error } = await (b as any).select();
      return { result, error };
    };

    // Direct fetch to the gateway — bypasses update_body wrapping entirely.
    // Tries top-level { data, set } then top-level { columns: [...] }.
    const directGatewayUpdate = async () => {
      const { url, apiKey, client, headers: extraHeaders } = deps.getConnectionConfig();
      const base = url.replace(/\/$/, "");
      const conditions = where.map((clause) => {
        const column = hasUppercase(clause.field) ? toSnakeCase(clause.field) : clause.field;
        const value = clause.value;
        switch (clause.operator) {
          case "ne": return { operator: "neq", column, value };
          case "contains": return { operator: "like", column, value: `%${value}%` };
          case "starts_with": return { operator: "like", column, value: `${String(value)}%` };
          case "ends_with": return { operator: "like", column, value: `%${String(value)}` };
          case "eq": return { operator: "eq", column, eq_column: column, value, eq_value: value };
          default: return { operator: clause.operator, column, value };
        }
      });
      const requestHeaders: Record<string, string> = {
        "Content-Type": "application/json",
        "apikey": apiKey,
        "x-api-key": apiKey,
        "X-Athena-Client": client ?? "railway_direct",
        "X-Backend-Type": "athena",
        "X-Strip-Nulls": "true",
        ...(extraHeaders ?? {}),
      };
      const payloads = [
        { table_name: model, data: updateData, set: updateData, conditions, strip_nulls: true },
        {
          table_name: model,
          columns: Object.entries(updateData).map(([column, value]) => ({ column, value })),
          conditions,
          strip_nulls: true,
        },
      ];
      for (const payload of payloads) {
        const res = await fetch(`${base}/gateway/update`, {
          method: "POST",
          headers: requestHeaders,
          body: JSON.stringify(payload),
        });
        const raw = await res.text();
        let parsed: any = null;
        try { parsed = JSON.parse(raw); } catch { parsed = raw; }
        const fetchError = parsed?.error ?? parsed?.message ?? null;
        const fetchData = parsed?.data ?? null;
        if (!fetchError || isSuccessMessageInError(fetchError)) {
          debugLog("direct_gateway_succeeded");
          return { result: fetchData, error: null as string | null };
        }
        debugLog("direct_gateway_shape_failed", { error: String(fetchError) });
        if (!String(fetchError).toLowerCase().includes("update payload required")) {
          return { result: null, error: String(fetchError) };
        }
      }
      return { result: null, error: "update payload required: all gateway formats exhausted" };
    };

    const isPayloadError = (e: unknown) =>
      String(e).toLowerCase().includes("update payload required");

    // 1. Plain update_body
    const first = await run(build("plain"));
    if (!first.error || isSuccessMessageInError(first.error)) {
      debugLog("primary_succeeded", { shape: "plain" });
      const row = Array.isArray(first.result) ? (first.result[0] as unknown) : (first.result as unknown);
      return (row ? mapRowToBetterAuth(row as T) : null) as T | null;
    }
    debugLog("primary_failed", { error: String(first.error) });
    if (!isPayloadError(first.error)) {
      throw new Error(`[AthenaAdapter] update on "${model}" failed: ${first.error}`);
    }

    // 2. Wrapped { data, set } update_body
    const second = await run(build("wrapped"));
    if (!second.error || isSuccessMessageInError(second.error)) {
      debugLog("wrapped_succeeded", { shape: "data/set" });
      const row = Array.isArray(second.result) ? (second.result[0] as unknown) : (second.result as unknown);
      return (row ? mapRowToBetterAuth(row as T) : null) as T | null;
    }
    debugLog("wrapped_failed", { error: String(second.error) });
    if (!isPayloadError(second.error)) {
      throw new Error(`[AthenaAdapter] update on "${model}" failed: ${second.error}`);
    }

    // 3. Direct fetch with top-level fields
    const direct = await directGatewayUpdate();
    if (direct.error && !isSuccessMessageInError(direct.error)) {
      debugLog("direct_gateway_final_failed", { error: String(direct.error) });
      throw new Error(`[AthenaAdapter] update on "${model}" failed: ${direct.error}`);
    }
    debugLog("direct_gateway_succeeded");
    const row = Array.isArray(direct.result)
      ? (direct.result[0] as unknown)
      : (direct.result as unknown);
    return (row ? mapRowToBetterAuth(row as T) : null) as T | null;
  };
}
