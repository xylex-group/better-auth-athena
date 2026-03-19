import type { WhereClause, AthenaFilterBuilder } from "../utils";
import { applyWhere, isSuccessMessageInError } from "../utils";

export type DeleteManyDeps = {
  ensureDbClient: () => any;
  headers?: Record<string, string>;
};

export function deleteManyMethod(deps: DeleteManyDeps) {
  const { ensureDbClient, headers } = deps;

  return async function deleteMany({
    model,
    where,
  }: {
    model: string;
    where: WhereClause[];
  }) {
    const db = ensureDbClient();
    const mainBuilder = db.from(model) as AthenaFilterBuilder;
    let builder = mainBuilder;

    for (const clause of where) {
      builder = applyWhere(
        builder,
        clause.field,
        clause.operator,
        clause.value,
      );
    }

    const { data: result, error } = await (builder as any).delete(
      headers ? ({ headers } as any) : undefined,
    ).select();

    if (error && !isSuccessMessageInError(error)) {
      throw new Error(
        `[AthenaAdapter] deleteMany on "${model}" failed: ${error}`,
      );
    }

    const deletedCount = Array.isArray(result) ? result.length : result ? 1 : 0;
    // Fallback: if the live gateway doesn't apply `in` conditions correctly,
    // delete rows one-by-one so counts are stable for e2e tests.
    const inClause = where.find(
      (c) => c.operator === "in" && c.value != null,
    );
    if (
      inClause &&
      Array.isArray(inClause.value) &&
      deletedCount < inClause.value.length
    ) {
      let n = 0;
      for (const v of inClause.value) {
        const b = db.from(model);
        const filtered = applyWhere(
          b as any,
          inClause.field,
          "eq",
          v,
        );
        const { data: rowData, error: rowErr } = await (filtered as any).delete(
          headers ? ({ headers } as any) : undefined,
        ).select();
        if (rowErr && isSuccessMessageInError(rowErr)) continue;
        n += Array.isArray(rowData) ? rowData.length : rowData ? 1 : 0;
      }
      return n;
    }

    return deletedCount;
  };
}
