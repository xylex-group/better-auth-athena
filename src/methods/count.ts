import type { WhereClause } from "../utils";
import {
  applyWhere,
  isMissingColumnError,
  snakeMapper,
  identityMapper,
} from "../utils";

export type CountDeps = {
  ensureDbClient: () => any;
};

export function countMethod(deps: CountDeps) {
  const { ensureDbClient } = deps;

  return async function count({
    model,
    where,
  }: {
    model: string;
    where?: WhereClause[];
  }) {
    const db = ensureDbClient();

    const run = async (columnMapper: (col: string) => string) => {
      let builder = db.from(model).select();

      if (where) {
        for (const clause of where) {
          builder = applyWhere(
            builder,
            clause.field,
            clause.operator,
            clause.value,
            columnMapper,
          );
        }
      }

      const { data: result, error } = await builder;
      return { result, error };
    };

    const first = await run(snakeMapper);
    if (first.error) {
      if (isMissingColumnError(first.error)) {
        const retry = await run(identityMapper);
        if (retry.error) {
          throw new Error(
            `[AthenaAdapter] count on "${model}" failed: ${retry.error}`,
          );
        }
        return Array.isArray(retry.result) ? retry.result.length : 0;
      }
      throw new Error(
        `[AthenaAdapter] count on "${model}" failed: ${first.error}`,
      );
    }

    return Array.isArray(first.result) ? first.result.length : 0;
  };
}
