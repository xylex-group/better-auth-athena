import type { SupabaseClient as AthenaClient } from "@xylex-group/athena";
import type { WhereClause } from "../utils";
import { applyWhere } from "../utils";

export type DeleteDeps = {
  ensureDbClient: () => AthenaClient;
};

export function deleteMethod(deps: DeleteDeps) {
  const { ensureDbClient } = deps;

  return async function del({
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

    const { error } = await builder.delete();

    if (error) {
      throw new Error(
        `[AthenaAdapter] delete on "${model}" failed: ${error}`,
      );
    }
  };
}
