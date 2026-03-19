/**
 * Real-database integration tests for the Athena adapter.
 *
 * Runs every adapter method (create, update, updateMany, delete, deleteMany,
 * findOne, findMany, count) against a live Athena gateway and database.
 *
 * Setup:
 * 1. Create the table (see tests/fixtures/athana_adattper_e2e.sql).
 * 2. Set ATHENA_URL and ATHENA_API_KEY (or use config.yaml with athena.url and athena.apiKey).
 * 3. Run: pnpm test -- athenaAdapter.real.e2e.test.ts
 *
 * Uses client "athena-logging" and table "athena_adapter_e2e".
 *
 * If ATHENA_URL or ATHENA_API_KEY is missing, the entire suite is skipped.
 */

import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";

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

// Do NOT mock @xylex-group/athena — use real client for real DB.
import { athenaAdapter } from "../src/index";

const MODEL = "athena_adapter_e2e";

function getConfig(): { url: string; apiKey: string } {
  const url = process.env.ATHENA_URL ?? "";
  const apiKey = process.env.ATHENA_API_KEY ?? "";
  if (!url || !apiKey) {
    throw new Error(
      "ATHENA_URL and ATHENA_API_KEY must be set for real e2e tests",
    );
  }
  return { url, apiKey };
}

const hasRealConfig =
  typeof process !== "undefined" &&
  process.env?.ATHENA_URL &&
  process.env?.ATHENA_API_KEY;

type Adapter = {
  create: (args: {
    model: string;
    data: Record<string, unknown>;
    select?: string[];
  }) => Promise<Record<string, unknown>>;
  update: (args: {
    model: string;
    where: Array<{ field: string; operator: string; value: unknown }>;
    update: Record<string, unknown>;
  }) => Promise<Record<string, unknown> | null>;
  updateMany: (args: {
    model: string;
    where: Array<{ field: string; operator: string; value: unknown }>;
    update: Record<string, unknown>;
  }) => Promise<number>;
  delete: (args: {
    model: string;
    where: Array<{ field: string; operator: string; value: unknown }>;
  }) => Promise<void>;
  deleteMany: (args: {
    model: string;
    where: Array<{ field: string; operator: string; value: unknown }>;
  }) => Promise<number>;
  findOne: (args: {
    model: string;
    where: Array<{ field: string; operator: string; value: unknown }>;
    select?: string[];
  }) => Promise<Record<string, unknown> | null>;
  findMany: (args: {
    model: string;
    where?: Array<{ field: string; operator: string; value: unknown }>;
    limit: number;
    offset?: number;
    select?: string[];
    sortBy?: { field: string; direction: "asc" | "desc" };
  }) => Promise<Record<string, unknown>[]>;
  count: (args: {
    model: string;
    where?: Array<{ field: string; operator: string; value: unknown }>;
  }) => Promise<number>;
};

describe.skipIf(!hasRealConfig)("athenaAdapter (real database e2e)", () => {
  let adapter: Adapter;
  let runId: string;
  const createdIds: string[] = [];

  beforeAll(() => {
    const { url, apiKey } = getConfig();
    const headers: Record<string, string> | undefined =
      process.env.ATHENA_E2E_X_USER_ID != null
        ? { "X-User-Id": process.env.ATHENA_E2E_X_USER_ID }
        : undefined;
    adapter = athenaAdapter({
      url,
      apiKey,
      client: "athena_logging",
      watchConfig: false,
      ...(headers && { headers }),
    }) as unknown as Adapter;
    runId = `e2e-${Date.now()}-${Math.random().toString(36).slice(2, 9)}`;
  });

  afterAll(async () => {
    if (!adapter || createdIds.length === 0) return;
    try {
      const n = await adapter.deleteMany({
        model: MODEL,
        where: [{ field: "id", operator: "in", value: createdIds }],
      });
      expect(n).toBeGreaterThanOrEqual(0);
    } catch {
      // Best-effort cleanup
    }
  });

  it("create: inserts a row and returns it in camelCase", async () => {
    const id = `${runId}-create`;
    createdIds.push(id);
    const createdAt = new Date().toISOString();
    const row = await adapter.create({
      model: MODEL,
      data: {
        id,
        name: "Alice",
        email: "alice@e2e.test",
        createdAt,
      },
    });
    expect(row).toBeDefined();
    expect(row.id).toBe(id);
    expect(row.name).toBe("Alice");
    expect(row.email).toBe("alice@e2e.test");
    expect((row as any).createdAt).toBeDefined();
  });

  it("findOne: returns row by id (eq), null for missing", async () => {
    const id = `${runId}-findOne`;
    createdIds.push(id);
    await adapter.create({
      model: MODEL,
      data: { id, name: "Bob", email: "bob@e2e.test" },
    });
    const found = await adapter.findOne({
      model: MODEL,
      where: [{ field: "id", operator: "eq", value: id }],
    });
    expect(found).not.toBeNull();
    expect(found!.id).toBe(id);
    expect(found!.name).toBe("Bob");

    const missing = await adapter.findOne({
      model: MODEL,
      where: [{ field: "id", operator: "eq", value: "nonexistent-id-xyz" }],
    });
    expect(missing).toBeNull();
  });

  it("findOne: select specific columns", async () => {
    const id = `${runId}-select`;
    createdIds.push(id);
    await adapter.create({
      model: MODEL,
      data: { id, name: "Carol", email: "carol@e2e.test" },
    });
    const row = await adapter.findOne({
      model: MODEL,
      where: [{ field: "id", operator: "eq", value: id }],
      select: ["id", "name"],
    });
    expect(row).not.toBeNull();
    expect(row!.id).toBe(id);
    expect(row!.name).toBe("Carol");
    expect((row as any).email).toBeUndefined();
  });

  it("findMany: limit, offset, sortBy", async () => {
    const base = `${runId}-fm`;
    const ids = [`${base}-1`, `${base}-2`, `${base}-3`];
    createdIds.push(...ids);
    await adapter.create({
      model: MODEL,
      data: { id: ids[0], name: "C", email: "c@e2e.test" },
    });
    await adapter.create({
      model: MODEL,
      data: { id: ids[1], name: "A", email: "a@e2e.test" },
    });
    await adapter.create({
      model: MODEL,
      data: { id: ids[2], name: "B", email: "b@e2e.test" },
    });

    const all = await adapter.findMany({
      model: MODEL,
      where: [{ field: "id", operator: "in", value: ids }],
      limit: 10,
      sortBy: { field: "name", direction: "asc" },
    });
    expect(all.length).toBe(3);
    expect(all.map((r) => r.name)).toEqual(["A", "B", "C"]);

    const page = await adapter.findMany({
      model: MODEL,
      where: [{ field: "id", operator: "in", value: ids }],
      limit: 2,
      offset: 1,
      sortBy: { field: "name", direction: "asc" },
    });
    expect(page.length).toBe(2);
    expect(page.map((r) => r.name)).toEqual(["B", "C"]);
  });

  it("findMany: where eq and in", async () => {
    const id = `${runId}-fmeq`;
    createdIds.push(id);
    await adapter.create({
      model: MODEL,
      data: { id, name: "Single", email: "single@e2e.test" },
    });
    const rows = await adapter.findMany({
      model: MODEL,
      where: [{ field: "name", operator: "eq", value: "Single" }],
      limit: 10,
    });
    expect(rows.length).toBe(1);
    expect(rows[0].name).toBe("Single");
  });

  it("update: updates one row", async () => {
    const id = `${runId}-upd`;
    createdIds.push(id);
    await adapter.create({
      model: MODEL,
      data: { id, name: "Old", email: "old@e2e.test" },
    });
    const updated = await adapter.update({
      model: MODEL,
      where: [{ field: "id", operator: "eq", value: id }],
      update: { name: "New" },
    });
    expect(updated).not.toBeNull();
    expect(updated!.name).toBe("New");

    const found = await adapter.findOne({
      model: MODEL,
      where: [{ field: "id", operator: "eq", value: id }],
    });
    expect(found!.name).toBe("New");
  });

  it("updateMany: updates multiple rows", async () => {
    const base = `${runId}-um`;
    const id1 = `${base}-1`;
    const id2 = `${base}-2`;
    createdIds.push(id1, id2);
    await adapter.create({
      model: MODEL,
      data: { id: id1, name: "UMA", email: "uma@e2e.test" },
    });
    await adapter.create({
      model: MODEL,
      data: { id: id2, name: "UMA", email: "umb@e2e.test" },
    });
    const n = await adapter.updateMany({
      model: MODEL,
      where: [{ field: "name", operator: "eq", value: "UMA" }],
      update: { name: "UM-Updated" },
    });
    expect(n).toBeGreaterThanOrEqual(2);

    const rows = await adapter.findMany({
      model: MODEL,
      where: [{ field: "id", operator: "in", value: [id1, id2] }],
      limit: 10,
    });
    expect(rows.every((r) => r.name === "UM-Updated")).toBe(true);
  });

  it("count: returns correct count", async () => {
    const base = `${runId}-cnt`;
    const id1 = `${base}-1`;
    const id2 = `${base}-2`;
    createdIds.push(id1, id2);
    await adapter.create({
      model: MODEL,
      data: { id: id1, name: "CountA", email: "ca@e2e.test" },
    });
    await adapter.create({
      model: MODEL,
      data: { id: id2, name: "CountA", email: "cb@e2e.test" },
    });

    const n = await adapter.count({
      model: MODEL,
      where: [{ field: "name", operator: "eq", value: "CountA" }],
    });
    expect(n).toBeGreaterThanOrEqual(2);

    const zero = await adapter.count({
      model: MODEL,
      where: [{ field: "id", operator: "eq", value: "nonexistent-count-id" }],
    });
    expect(zero).toBe(0);
  });

  it("delete: removes one row", async () => {
    const id = `${runId}-del`;
    createdIds.push(id);
    await adapter.create({
      model: MODEL,
      data: { id, name: "ToDelete", email: "del@e2e.test" },
    });
    await adapter.delete({
      model: MODEL,
      where: [{ field: "id", operator: "eq", value: id }],
    });
    const idx = createdIds.indexOf(id);
    if (idx !== -1) createdIds.splice(idx, 1);

    const found = await adapter.findOne({
      model: MODEL,
      where: [{ field: "id", operator: "eq", value: id }],
    });
    expect(found).toBeNull();
  });

  it("deleteMany: removes multiple rows", async () => {
    const base = `${runId}-dm`;
    const id1 = `${base}-1`;
    const id2 = `${base}-2`;
    createdIds.push(id1, id2);
    await adapter.create({
      model: MODEL,
      data: { id: id1, name: "DM1", email: "dm1@e2e.test" },
    });
    await adapter.create({
      model: MODEL,
      data: { id: id2, name: "DM2", email: "dm2@e2e.test" },
    });
    const n = await adapter.deleteMany({
      model: MODEL,
      where: [{ field: "id", operator: "in", value: [id1, id2] }],
    });
    expect(n).toBeGreaterThanOrEqual(2);
    createdIds.splice(createdIds.indexOf(id1), 1);
    createdIds.splice(createdIds.indexOf(id2), 1);

    const count = await adapter.count({
      model: MODEL,
      where: [{ field: "id", operator: "in", value: [id1, id2] }],
    });
    expect(count).toBe(0);
  });
});
