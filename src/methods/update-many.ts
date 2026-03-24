import type { WhereClause, AthenaFilterBuilder } from "../utils";
import {
  toDbRecord,
  applyWhere,
  isSuccessMessageInError,
  toSnakeCase,
  hasUppercase,
} from "../utils";
import type { GatewayConnectionConfig } from "./update";

export type UpdateManyDeps = {
  ensureDbClient: () => any;
  getConnectionConfig: () => GatewayConnectionConfig;
};

export function updateManyMethod(deps: UpdateManyDeps) {
  const { ensureDbClient } = deps;

  return async function updateMany({
    model,
    where,
    update,
  }: {
    model: string;
    where: WhereClause[];
    update: Record<string, unknown>;
  }) {
    const db = ensureDbClient();
    const updateData = toDbRecord(update);
    const debugUpdates = process.env.ATHENA_ADAPTER_DEBUG_UPDATES === "1";
    const debugLog = (event: string, extra?: Record<string, unknown>) => {
      if (!debugUpdates) return;
      console.info("[AthenaAdapter][updateMany]", { event, model, ...extra });
    };

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

    const directGatewayUpdateMany = async () => {
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

    const countResult = (r: unknown) =>
      Array.isArray(r) ? r.length : r ? 1 : 0;

    const isPayloadError = (e: unknown) =>
      String(e).toLowerCase().includes("update payload required");

    // 1. Plain
    const first = await run(build("plain"));
    if (!first.error || isSuccessMessageInError(first.error)) {
      debugLog("primary_succeeded", { shape: "plain" });
      return countResult(first.result);
    }
    debugLog("primary_failed", { error: String(first.error) });
    if (!isPayloadError(first.error)) {
      throw new Error(`[AthenaAdapter] updateMany on "${model}" failed: ${first.error}`);
    }

    // 2. Wrapped
    const second = await run(build("wrapped"));
    if (!second.error || isSuccessMessageInError(second.error)) {
      debugLog("wrapped_succeeded", { shape: "data/set" });
      return countResult(second.result);
    }
    debugLog("wrapped_failed", { error: String(second.error) });
    if (!isPayloadError(second.error)) {
      throw new Error(`[AthenaAdapter] updateMany on "${model}" failed: ${second.error}`);
    }

    // 3. Direct fetch
    const direct = await directGatewayUpdateMany();
    if (direct.error && !isSuccessMessageInError(direct.error)) {
      debugLog("direct_gateway_final_failed", { error: String(direct.error) });
      throw new Error(`[AthenaAdapter] updateMany on "${model}" failed: ${direct.error}`);
    }
    debugLog("direct_gateway_succeeded");
    return countResult(direct.result);
  };
}
