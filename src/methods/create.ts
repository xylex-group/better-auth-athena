import type { SupabaseClient as AthenaClient } from "@xylex-group/athena";
import { toDbRecord, mapRowToBetterAuth } from "../utils";

export type CreateDeps = {
  ensureDbClient: () => AthenaClient;
};

export function createMethod(deps: CreateDeps) {
  const { ensureDbClient } = deps;

  return async function create<T extends Record<string, unknown>>({
    model,
    data,
  }: {
    model: string;
    data: T;
    select?: string[];
  }) {
    const db = ensureDbClient();
    const insertData = toDbRecord(data);
    const { data: result, error } = await db
      .from(model)
      .insert(insertData)
      .select();

    if (error) {
      throw new Error(
        `[AthenaAdapter] create on "${model}" failed: ${error}`,
      );
    }

    const row = Array.isArray(result) ? result[0] : result;
    return mapRowToBetterAuth((row ?? insertData) as T);
  };
}
