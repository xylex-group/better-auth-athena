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

    const run = async (
      columnMapper: (col: string) => string,
      opts?: { skipWhere?: boolean; limitOverride?: number; offsetOverride?: number },
    ) => {
      const columns =
        select && select.length > 0
          ? select.map((c) => columnMapper(c)).join(", ")
          : undefined;

      let builder = db.from(model).select(columns);

      if (!opts?.skipWhere && where) {
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

      const effectiveLimit = opts?.limitOverride ?? limit;
      const effectiveOffset = opts?.offsetOverride ?? offset;

      if (effectiveLimit !== undefined) {
        builder = builder.limit(effectiveLimit);
      }

      if (effectiveOffset !== undefined) {
        builder = builder.offset(effectiveOffset);
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
      const betterAuthRows = rows.map((r) => mapRowToBetterAuth(r)) as unknown as T[];

      if (!where?.length) return applySort(betterAuthRows);

      const filtered = filterRowsByWhere(
        betterAuthRows as unknown as Record<string, unknown>[],
        where,
      ) as unknown as T[];

      // If filtering changed the row set, the gateway likely ignored `where`
      // (or applied it before limiting/offset). In that case, re-apply sorting
      // and slice using the requested offset/limit.
      if (filtered.length !== betterAuthRows.length) {
        const off = offset ?? 0;
        const end = limit !== undefined ? off + limit : undefined;
        return applySort(filtered).slice(off, end);
      }

      return applySort(filtered);
    };

    const postFilterAndSlice = (rows: Record<string, unknown>[]) => {
      const mappedSorted = mapAndSort(rows);
      const off = offset ?? 0;
      const end = limit !== undefined ? off + limit : undefined;
      return mappedSorted.slice(off, end);
    };

    if (first.error) {
      if (isMissingColumnError(first.error)) {
        const retry = await run(identityMapper);
        if (retry.error) {
          throw new Error(
            `[AthenaAdapter] findMany on "${model}" failed: ${retry.error}`,
          );
        }
        const retryRows = pickRows(retry.result);
        // Decisive fallback: if gateway-side `where` yields empty/insufficient rows,
        // fetch a broader candidate set and apply `where`/sort/offset/limit in-memory.
        if (where?.length) {
          const broad = await run(identityMapper, {
            skipWhere: true,
            limitOverride: Math.max((offset ?? 0) + (limit ?? 0) + 500, 5000),
            offsetOverride: 0,
          });
          if (!broad.error) return postFilterAndSlice(pickRows(broad.result));
        }
        return mapAndSort(retryRows);
      }

      throw new Error(
        `[AthenaAdapter] findMany on "${model}" failed: ${first.error}`,
      );
    }

    const firstRows = pickRows(first.result);
    if (where?.length) {
      const broad = await run(snakeMapper, {
        skipWhere: true,
        limitOverride: Math.max((offset ?? 0) + (limit ?? 0) + 500, 5000),
        offsetOverride: 0,
      });
      if (!broad.error) return postFilterAndSlice(pickRows(broad.result));
    }
    return mapAndSort(firstRows);
  };
}
