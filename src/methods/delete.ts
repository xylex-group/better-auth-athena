import type { WhereClause, AthenaFilterBuilder } from "../utils";
import { applyWhere, isSuccessMessageInError } from "../utils";

export type DeleteDeps = {
  ensureDbClient: () => any;
  headers?: Record<string, string>;
};

export function deleteMethod(deps: DeleteDeps) {
  const { ensureDbClient, headers } = deps;

  return async function del({
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

    const { error } = await (builder as any).delete(
      headers ? ({ headers } as any) : undefined,
    );

    if (error && !isSuccessMessageInError(error)) {
      throw new Error(
        `[AthenaAdapter] delete on "${model}" failed: ${error}`,
      );
    }
  };
}
