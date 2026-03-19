import type { WhereClause, AthenaFilterBuilder } from "../utils";
import { applyWhere } from "../utils";

export type DeleteManyDeps = {
  ensureDbClient: () => any;
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
    let builder = db.from(model) as AthenaFilterBuilder;

    for (const clause of where) {
      builder = applyWhere(
        builder,
        clause.field,
        clause.operator,
        clause.value,
      );
    }

    const { data: result, error } = await (builder as any).delete().select();

    if (error) {
      throw new Error(
        `[AthenaAdapter] deleteMany on "${model}" failed: ${error}`,
      );
    }

    return Array.isArray(result) ? result.length : result ? 1 : 0;
  };
}
