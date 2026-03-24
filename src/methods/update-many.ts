import type { WhereClause, AthenaFilterBuilder } from "../utils";
import { toDbRecord, applyWhere, isSuccessMessageInError } from "../utils";

export type UpdateManyDeps = {
  ensureDbClient: () => any;
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
      console.info("[AthenaAdapter][updateMany]", {
        event,
        model,
        ...extra,
      });
    };
    const build = (useRetryShape: boolean) => {
      if (!useRetryShape) {
        let b = db.from(model).update(updateData as any) as AthenaFilterBuilder;
        for (const clause of where) {
          b = applyWhere(b, clause.field, clause.operator, clause.value);
        }
        return b;
      }

      let b = db
        .from(model)
        .update(updateData as any, {
          updateBody: { data: updateData, set: updateData },
        } as any) as AthenaFilterBuilder;
      for (const clause of where) {
        b = applyWhere(b, clause.field, clause.operator, clause.value);
      }
      return b;
    };

    const run = async (b: AthenaFilterBuilder) => {
      const { data: result, error } = await (b as any).select();
      return { result, error };
    };

    const first = await run(build(false));
    if (first.error && !isSuccessMessageInError(first.error)) {
      debugLog("primary_failed", { error: String(first.error) });
      const msg = String(first.error);
      if (msg.toLowerCase().includes("update payload required")) {
        const retry = await run(build(true));
        if (retry.error && !isSuccessMessageInError(retry.error)) {
          debugLog("retry_failed", { error: String(retry.error) });
          throw new Error(
            `[AthenaAdapter] updateMany on "${model}" failed: ${retry.error}`,
          );
        }
        debugLog("retry_succeeded", { shape: "updateBody(data/set)" });
        return Array.isArray(retry.result) ? retry.result.length : retry.result ? 1 : 0;
      }

      throw new Error(
        `[AthenaAdapter] updateMany on "${model}" failed: ${first.error}`,
      );
    }

    debugLog("primary_succeeded", { shape: "plain" });

    return Array.isArray(first.result) ? first.result.length : first.result ? 1 : 0;
  };
}
