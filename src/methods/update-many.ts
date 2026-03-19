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
    const build = (useRetryShape: boolean) => {
      if (!useRetryShape) {
        let b = db
          .from(model)
          .update({ data: updateData, set: updateData } as any) as AthenaFilterBuilder;
        for (const clause of where) {
          b = applyWhere(b, clause.field, clause.operator, clause.value);
        }
        return b;
      }

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
            `[AthenaAdapter] updateMany on "${model}" failed: ${retry.error}`,
          );
        }
        return Array.isArray(retry.result) ? retry.result.length : retry.result ? 1 : 0;
      }

      throw new Error(
        `[AthenaAdapter] updateMany on "${model}" failed: ${first.error}`,
      );
    }

    return Array.isArray(first.result) ? first.result.length : first.result ? 1 : 0;
  };
}
