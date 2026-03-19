import type { WhereClause } from "../utils";
import {
  applyWhere,
  mapRowToBetterAuth,
  isMissingColumnError,
  snakeMapper,
  identityMapper,
} from "../utils";

export type FindOneDeps = {
  ensureDbClient: () => any;
};

export function findOneMethod(deps: FindOneDeps) {
  const { ensureDbClient } = deps;

  return async function findOne<T>({
    model,
    where,
    select,
  }: {
    model: string;
    where: WhereClause[];
    select?: string[];
    join?: unknown;
  }) {
    const db = ensureDbClient();

    const run = async (columnMapper: (col: string) => string) => {
      const columns =
        select && select.length > 0
          ? select.map((c) => columnMapper(c)).join(", ")
          : undefined;

      let builder = db.from(model).select(columns);

      for (const clause of where) {
        builder = applyWhere(
          builder,
          clause.field,
          clause.operator,
          clause.value,
          columnMapper,
        );
      }

      const { data: result, error } = await builder.limit(1);
      return { result, error };
    };

    const first = await run(snakeMapper);
    if (first.error) {
      if (isMissingColumnError(first.error)) {
        const retry = await run(identityMapper);
        if (retry.error) {
          throw new Error(
            `[AthenaAdapter] findOne on "${model}" failed: ${retry.error}`,
          );
        }

        const rows = Array.isArray(retry.result)
          ? retry.result
          : retry.result
            ? [retry.result]
            : [];
        const row = rows[0] ?? null;
        return (row ? mapRowToBetterAuth(row as T) : null) as T | null;
      }

      throw new Error(
        `[AthenaAdapter] findOne on "${model}" failed: ${first.error}`,
      );
    }

    const rows = Array.isArray(first.result)
      ? first.result
      : first.result
        ? [first.result]
        : [];
    const row = rows[0] ?? null;
    return (row ? mapRowToBetterAuth(row as T) : null) as T | null;
  };
}
