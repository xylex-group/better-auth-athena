import type { SupabaseClient as AthenaClient } from "@xylex-group/athena";
import type { WhereClause } from "../utils";
import { applyWhere } from "../utils";

export type DeleteManyDeps = {
  ensureDbClient: () => AthenaClient;
};

export function deleteManyMethod(deps: DeleteManyDeps) {
  const { ensureDbClient } = deps;

  return async function deleteMany({
    model,
    where,
  }: {
    model: string;
    where: WhereClause[];
  }) {
    const db = ensureDbClient();
    let builder = db.from(model);

    for (const clause of where) {
      builder = applyWhere(
        builder,
        clause.field,
        clause.operator,
        clause.value,
      );
    }

    const { data: result, error } = await builder.delete().select();

    if (error) {
      throw new Error(
        `[AthenaAdapter] deleteMany on "${model}" failed: ${error}`,
      );
    }

    return Array.isArray(result) ? result.length : result ? 1 : 0;
  };
}
