import type { WhereClause } from "../utils";
import {
  applyWhere,
  mapRowToBetterAuth,
  isMissingColumnError,
  snakeMapper,
  identityMapper,
  filterRowsByWhere,
} from "../utils";

export type FindManyDeps = {
  ensureDbClient: () => any;
};

export function findManyMethod(deps: FindManyDeps) {
  const { ensureDbClient } = deps;

  return async function findMany<T>({
    model,
    where,
    limit,
    sortBy,
    offset,
    select,
  }: {
    model: string;
    where?: WhereClause[];
    limit: number;
    select?: string[];
    sortBy?: { field: string; direction: "asc" | "desc" };
    offset?: number;
    join?: unknown;
  }) {
    const db = ensureDbClient();

    const run = async (columnMapper: (col: string) => string) => {
      const columns =
        select && select.length > 0
          ? select.map((c) => columnMapper(c)).join(", ")
          : undefined;

      let builder = db.from(model).select(columns);

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

      if (limit !== undefined) {
        builder = builder.limit(limit);
      }

      if (offset !== undefined) {
        builder = builder.offset(offset);
      }

      const { data: result, error } = await builder;
      return { result, error };
    };

    const first = await run(snakeMapper);
    const pickRows = (res: unknown) =>
      (Array.isArray(res) ? res : []) as Record<string, unknown>[];

    const applySort = (rows: T[]) => {
      if (!sortBy) return rows;
      const sortField = sortBy.field;
      rows.sort((a, b) => {
        const aVal = (a as Record<string, unknown>)[sortField];
        const bVal = (b as Record<string, unknown>)[sortField];
        if (aVal == null && bVal == null) return 0;
        if (aVal == null) return sortBy.direction === "asc" ? -1 : 1;
        if (bVal == null) return sortBy.direction === "asc" ? 1 : -1;
        const cmp =
          typeof aVal === "string" && typeof bVal === "string"
            ? aVal.localeCompare(bVal)
            : aVal < bVal
              ? -1
              : aVal > bVal
                ? 1
                : 0;
        return sortBy.direction === "asc" ? cmp : -cmp;
      });
      return rows;
    };

    const mapAndSort = (rows: Record<string, unknown>[]) => {
      const betterAuthRows = rows.map((r) =>
        mapRowToBetterAuth(r),
      ) as unknown as T[];
      return applySort(betterAuthRows);
    };

    const applyLimitOffset = (rows: T[]) => {
      if (limit === undefined && offset === undefined) return rows;
      const off = offset ?? 0;
      const end = limit !== undefined ? off + limit : undefined;
      return rows.slice(off, end);
    };

    const processRows = (rawRows: Record<string, unknown>[]) => {
      const filtered = where?.length
        ? filterRowsByWhere(rawRows, where)
        : rawRows;
      return applyLimitOffset(mapAndSort(filtered));
    };

    if (first.error) {
      if (isMissingColumnError(first.error)) {
        const retry = await run(identityMapper);
        if (retry.error) {
          throw new Error(
            `[AthenaAdapter] findMany on "${model}" failed: ${retry.error}`,
          );
        }
        return processRows(pickRows(retry.result));
      }

      throw new Error(
        `[AthenaAdapter] findMany on "${model}" failed: ${first.error}`,
      );
    }

    return processRows(pickRows(first.result));
  };
}
