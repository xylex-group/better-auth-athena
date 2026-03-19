import { beforeEach, describe, expect, it, vi } from "vitest";

// `bun test` doesn't provide `vi.hoisted`, but Vitest does.
// Provide a tiny shim so the same tests can run under both runners.
if (typeof (vi as any).hoisted !== "function") {
  (vi as any).hoisted = (fn: () => unknown) => fn();
}

const { createAdapterFactory, createClient } = vi.hoisted(() => {
  return {
    createAdapterFactory: vi.fn((options: { adapter: () => unknown }) =>
      options.adapter(),
    ),
    createClient: vi.fn(),
  };
});

vi.mock("better-auth/adapters", () => ({
  createAdapterFactory,
}));

vi.mock("@xylex-group/athena", () => ({
  createClient,
}));

import { athenaAdapter } from "../src/index";

type BuilderResult = { data: unknown; error: unknown };

/** Adapter instance returned by our mock (createAdapterFactory returns options.adapter()). */
type TestAdapter = {
  create: (args: {
    model: string;
    data: Record<string, unknown>;
  }) => Promise<Record<string, unknown>>;
  update: (args: {
    model: string;
    update: Record<string, unknown>;
    where: Array<{ field: string; operator: string; value: unknown }>;
  }) => Promise<unknown>;
  findMany: (args: {
    model: string;
    limit: number;
    offset?: number;
    select?: string[];
    sortBy?: { field: string; direction: "asc" | "desc" };
  }) => Promise<Record<string, unknown>[]>;
};

const createBuilder = (result: BuilderResult) => {
  const calls: Array<{ method: string; args?: unknown[] }> = [];

  const builder = {
    calls,
    select: (columns?: string) => {
      calls.push({ method: "select", args: [columns] });
      return builder;
    },
    insert: (data: unknown) => {
      calls.push({ method: "insert", args: [data] });
      return builder;
    },
    update: (data: unknown) => {
      calls.push({ method: "update", args: [data] });
      return builder;
    },
    delete: () => {
      calls.push({ method: "delete" });
      return builder;
    },
    limit: (value: number) => {
      calls.push({ method: "limit", args: [value] });
      return builder;
    },
    offset: (value: number) => {
      calls.push({ method: "offset", args: [value] });
      return builder;
    },
    eq: (col: string, val: unknown) => {
      calls.push({ method: "eq", args: [col, val] });
      return builder;
    },
    neq: (col: string, val: unknown) => {
      calls.push({ method: "neq", args: [col, val] });
      return builder;
    },
    gt: (col: string, val: unknown) => {
      calls.push({ method: "gt", args: [col, val] });
      return builder;
    },
    gte: (col: string, val: unknown) => {
      calls.push({ method: "gte", args: [col, val] });
      return builder;
    },
    lt: (col: string, val: unknown) => {
      calls.push({ method: "lt", args: [col, val] });
      return builder;
    },
    lte: (col: string, val: unknown) => {
      calls.push({ method: "lte", args: [col, val] });
      return builder;
    },
    in: (col: string, vals: unknown[]) => {
      calls.push({ method: "in", args: [col, vals] });
      return builder;
    },
    not: (col: string, op?: string, val?: unknown) => {
      calls.push({ method: "not", args: [col, op, val] });
      return builder;
    },
    like: (col: string, val: string) => {
      calls.push({ method: "like", args: [col, val] });
      return builder;
    },
    then: (
      resolve: (value: BuilderResult) => unknown,
      reject?: (reason: unknown) => unknown,
    ) => Promise.resolve(result).then(resolve, reject),
  };

  return builder;
};

describe("athenaAdapter", () => {
  beforeEach(() => {
    createAdapterFactory.mockClear();
    createClient.mockClear();
  });

  it("creates records with the configured Athena client", async () => {
    const builder = createBuilder({
      data: [{ id: "1", name: "Ada" }],
      error: null,
    });
    const from = vi.fn(() => builder);
    createClient.mockReturnValue({ from });

    const adapter = athenaAdapter({
      url: "https://mirror1.athena-db.com",
      apiKey: "secret",
      client: "web",
    }) as unknown as TestAdapter;

    const result = await adapter.create({
      model: "users",
      data: { name: "Ada" },
    });

    expect(result).toEqual({ id: "1", name: "Ada" });
    expect(createClient).toHaveBeenCalledWith(
      "https://mirror1.athena-db.com",
      "secret",
      { client: "web" },
    );
    expect(from).toHaveBeenCalledWith("users");
    expect(builder.calls).toEqual(
      expect.arrayContaining([
        { method: "insert", args: [{ name: "Ada" }] },
        { method: "select", args: [undefined] },
      ]),
    );
  });

  it("applies where filters for updates", async () => {
    const builder = createBuilder({ data: [{ id: "1" }], error: null });
    const from = vi.fn(() => builder);
    createClient.mockReturnValue({ from });

    const adapter = athenaAdapter({
      url: "https://mirror1.athena-db.com",
      apiKey: "secret",
    }) as unknown as TestAdapter;

    await adapter.update({
      model: "users",
      update: { name: "Sam" },
      where: [
        { field: "name", operator: "contains", value: "sam" },
        { field: "id", operator: "not_in", value: ["1", "2"] },
      ],
    });

    expect(builder.calls).toEqual(
      expect.arrayContaining([
        { method: "update", args: [{ data: { name: "Sam" }, set: { name: "Sam" } }] },
        { method: "like", args: ["name", "%sam%"] },
        { method: "not", args: ["id", "in", ["1", "2"]] },
      ]),
    );
  });

  it("sorts findMany results when sortBy is supplied", async () => {
    const builder = createBuilder({
      data: [
        { id: "2", name: "Zoe" },
        { id: "1", name: "Ada" },
      ],
      error: null,
    });
    const from = vi.fn(() => builder);
    createClient.mockReturnValue({ from });

    const adapter = athenaAdapter({
      url: "https://mirror1.athena-db.com",
      apiKey: "secret",
    }) as unknown as TestAdapter;

    const result = await adapter.findMany({
      model: "users",
      limit: 5,
      offset: 0,
      select: ["id", "name"],
      sortBy: { field: "name", direction: "asc" },
    });

    expect(result.map((row) => row.name)).toEqual(["Ada", "Zoe"]);
    expect(builder.calls).toEqual(
      expect.arrayContaining([
        { method: "select", args: ["id, name"] },
        { method: "limit", args: [5] },
        { method: "offset", args: [0] },
      ]),
    );
  });
});
