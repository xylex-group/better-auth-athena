/**
 * Diagnostic real-e2e tests to determine whether failures are due to:
 * 1) the Athena gateway / database semantics, or
 * 2) the Better-Auth adapter logic (payload shapes / mapping).
 *
 * The tests compare direct `@xylex-group/athena` client calls with
 * adapter calls against the same rows and the same where/limit/update inputs.
 *
 * Notes:
 * - Requires `ATHENA_URL` and `ATHENA_API_KEY`.
 * - Assumes `tests/fixtures/athena_adapter_e2e.sql` table exists.
 */

import { describe, expect, it, vi } from "vitest";

if (typeof (vi as any).hoisted !== "function") {
  (vi as any).hoisted = (fn: () => unknown) => fn();
}

const { createAdapterFactory } = vi.hoisted(() => ({
  createAdapterFactory: (options: { adapter: () => unknown }) =>
    options.adapter(),
}));

vi.mock("better-auth/adapters", () => ({
  createAdapterFactory,
}));

import { createClient } from "@xylex-group/athena";
import { athenaAdapter } from "../src/index";

const MODEL = "athena_adapter_e2e";

function getConfig(): { url: string; apiKey: string } {
  const url = process.env.ATHENA_URL ?? "";
  const apiKey = process.env.ATHENA_API_KEY ?? "";
  return { url, apiKey };
}

const hasRealConfig = Boolean(
  typeof process !== "undefined" && process.env?.ATHENA_URL && process.env?.ATHENA_API_KEY,
);

type Adapter = ReturnType<typeof athenaAdapter> extends infer F
  ? F extends (...args: any[]) => any
    ? Awaited<ReturnType<F>>
    : never
  : never;

const normalizeRows = (rows: any[]) =>
  rows.map((r) => ({
    id: r.id,
    name: r.name,
    email: r.email,
  }));

describe("athena gateway diagnostics (real)", () => {
  if (!hasRealConfig) {
    it.skip("skipped: ATHENA_URL/ATHENA_API_KEY not set", () => {});
    return;
  }

  const { url, apiKey } = getConfig();
  const clientName = process.env.ATHENA_CLIENT ?? "athena_logging";
  const headers: Record<string, string> = {};
  const userId = process.env.ATHENA_E2E_X_USER_ID ?? clientName;
  const companyId = process.env.ATHENA_E2E_X_COMPANY_ID ?? userId;

  headers["X-User-Id"] = userId;
  headers["X-Company-Id"] = companyId;
  headers["X-Organization-Id"] = companyId;

  const direct = createClient(url, apiKey, {
    client: clientName,
    headers,
  });

  const adapter = athenaAdapter({
    url,
    apiKey,
    client: clientName,
    watchConfig: false,
    headers,
  }) as any;

  const runId = `diag-${Date.now()}-${Math.random().toString(36).slice(2, 9)}`;

  it("findMany where+limit: gateway semantics vs adapter", async () => {
    const ids = [`${runId}-fm-1`, `${runId}-fm-2`, `${runId}-fm-3`];
    const rowsToInsert = [
      { id: ids[0], name: "C", email: "c@diag.test" },
      { id: ids[1], name: "A", email: "a@diag.test" },
      { id: ids[2], name: "B", email: "b@diag.test" },
    ];

    for (const r of rowsToInsert) {
      await direct.from(MODEL).insert(r as any);
    }

    // Direct gateway:
    const directRes = await direct
      .from(MODEL)
      .select()
      .in("id", ids)
      .limit(10);
    expect(directRes.error).toBeNull();

    const directRows = normalizeRows(Array.isArray(directRes.data) ? directRes.data : []);
    // This assertion is decisive: if gateway ignores where, you'll see != 3.
    expect(directRows.length).toBe(3);

    const adapterRows = await adapter.findMany({
      model: MODEL,
      where: [{ field: "id", operator: "in", value: ids }],
      limit: 10,
      sortBy: { field: "name", direction: "asc" },
    });

    // If adapter is wrong but gateway is right, adapterRows length/order will diverge.
    expect(adapterRows.length).toBe(3);
    expect(adapterRows.map((r: any) => r.name)).toEqual(["A", "B", "C"]);
  }, 20_000);

  it("findMany where eq: gateway semantics vs adapter", async () => {
    const id = `${runId}-eq-1`;
    await direct.from(MODEL).insert({
      id,
      name: "Single",
      email: "single@diag.test",
    } as any);

    const directRes = await direct
      .from(MODEL)
      .select()
      .eq("name", "Single")
      .limit(10);

    expect(directRes.error).toBeNull();
    const directRows = normalizeRows(Array.isArray(directRes.data) ? directRes.data : []);
    expect(directRows.length).toBe(1);

    const adapterRows = await adapter.findMany({
      model: MODEL,
      where: [{ field: "name", operator: "eq", value: "Single" }],
      limit: 10,
    });
    expect(adapterRows.length).toBe(1);
    expect(adapterRows[0].name).toBe("Single");
  }, 20_000);

  it("update payload: gateway semantics vs adapter", async () => {
    const id = `${runId}-upd-1`;
    await direct.from(MODEL).insert({
      id,
      name: "Old",
      email: "old@diag.test",
    } as any);

    // Direct gateway update attempt using the plain values shape.
    const directUpdate = await direct
      .from(MODEL)
      .update({ name: "New" } as any)
      .eq("id", id)
      .select();

    // If this fails, the problem is the gateway/update contract, not the adapter.
    expect(directUpdate.error).toBeNull();

    const directAfter = await direct
      .from(MODEL)
      .select()
      .eq("id", id)
      .limit(1);
    const directAfterRows = normalizeRows(Array.isArray(directAfter.data) ? directAfter.data : []);
    expect(directAfterRows[0].name).toBe("New");

    // Now adapter update against the same row.
    const adapterUpdated = await adapter.update({
      model: MODEL,
      where: [{ field: "id", operator: "eq", value: id }],
      update: { name: "AdapterNew" },
    });

    expect(adapterUpdated).not.toBeNull();
    expect((adapterUpdated as any).name).toBe("AdapterNew");
  }, 20_000);
});

