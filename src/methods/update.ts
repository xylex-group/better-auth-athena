import type { WhereClause, AthenaFilterBuilder } from "../utils";
import {
  toDbRecord,
  mapRowToBetterAuth,
  applyWhere,
  isSuccessMessageInError,
} from "../utils";

export type UpdateDeps = {
  ensureDbClient: () => any;
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
    const build = (useRetryShape: boolean) => {
      // Primary shape: some gateway versions accept back-compat update_body keys.
      if (!useRetryShape) {
        let b = db
          .from(model)
          .update({ data: updateData, set: updateData } as any) as AthenaFilterBuilder;
        for (const clause of where) {
          b = applyWhere(b, clause.field, clause.operator, clause.value);
        }
        return b;
      }

      // Retry shape: send the plain update values, but also provide an explicit `updateBody`.
      let b = db
        .from(model)
        .update(updateData, {
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
      const msg = String(first.error);
      if (msg.toLowerCase().includes("update payload required")) {
        const retry = await run(build(true));
        if (retry.error && !isSuccessMessageInError(retry.error)) {
          throw new Error(
            `[AthenaAdapter] update on "${model}" failed: ${retry.error}`,
          );
        }
        const row = Array.isArray(retry.result)
          ? (retry.result[0] as unknown)
          : (retry.result as unknown);
        return (row ? mapRowToBetterAuth(row as T) : null) as T | null;
      }

      throw new Error(`[AthenaAdapter] update on "${model}" failed: ${first.error}`);
    }

    const row = Array.isArray(first.result)
      ? (first.result[0] as unknown)
      : (first.result as unknown);
    return (row ? mapRowToBetterAuth(row as T) : null) as T | null;
  };
}
