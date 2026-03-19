import type { WhereClause, AthenaFilterBuilder } from "../utils";
import { toDbRecord, mapRowToBetterAuth, applyWhere } from "../utils";

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
    let builder = db
      .from(model)
      .update({ data: updateData, set: updateData }) as AthenaFilterBuilder;

    for (const clause of where) {
      builder = applyWhere(
        builder,
        clause.field,
        clause.operator,
        clause.value,
      );
    }

    const { data: result, error } = await (builder as any).select();

    if (error) {
      throw new Error(`[AthenaAdapter] update on "${model}" failed: ${error}`);
    }

    const row = Array.isArray(result) ? result[0] : result;
    return (row ? mapRowToBetterAuth(row as T) : null) as T | null;
  };
}
