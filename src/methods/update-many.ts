import type { SupabaseClient as AthenaClient } from "@xylex-group/athena";
import type { WhereClause } from "../utils";
import { toDbRecord, applyWhere } from "../utils";

export type UpdateManyDeps = {
  ensureDbClient: () => AthenaClient;
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
    let builder = db
      .from(model)
      .update({ data: updateData, set: updateData });

    for (const clause of where) {
      builder = applyWhere(
        builder,
        clause.field,
        clause.operator,
        clause.value,
      );
    }

    const { data: result, error } = await builder.select();

    if (error) {
      throw new Error(
        `[AthenaAdapter] updateMany on "${model}" failed: ${error}`,
      );
    }

    return Array.isArray(result) ? result.length : result ? 1 : 0;
  };
}
