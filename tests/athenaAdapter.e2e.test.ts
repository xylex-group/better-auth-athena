import { beforeEach, describe, expect, it, vi } from "vitest";

const { createAdapterFactory, createClient } = vi.hoisted(() => {
  return {
    createAdapterFactory: vi.fn((options: { adapter: () => unknown }) => options.adapter()),
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

type TestAdapter = {
  create: (args: { model: string; data: Record<string, unknown> }) => Promise<Record<string, unknown>>;
  findOne: (args: {
    model: string;
    where: Array<{ field: string; operator: string; value: unknown }>;
    select?: string[];
  }) => Promise<Record<string, unknown> | null>;
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
    limit: (value: number) => {
      calls.push({ method: "limit", args: [value] });
      return builder;
    },
    eq: (col: string, val: unknown) => {
      calls.push({ method: "eq", args: [col, val] });
      return builder;
    },
    then: (
      resolve: (value: BuilderResult) => unknown,
      reject?: (reason: unknown) => unknown,
    ) => Promise.resolve(result).then(resolve, reject),
  };

  return builder;
};

describe("athenaAdapter (e2e)", () => {
  beforeEach(() => {
    createAdapterFactory.mockClear();
    createClient.mockClear();
  });

  it("writes snake_case + coerces ISO dates to Date and reads camelCase back", async () => {
    const builder = createBuilder({
      data: [
        {
          id: "m_1",
          user_id: "u_1",
          created_at: "2026-03-18T18:33:46.451Z",
        },
      ],
      error: null,
    });
    const from = vi.fn(() => builder);
    createClient.mockReturnValue({ from });

    const adapter = athenaAdapter({
      url: "https://athena-db.com",
      apiKey: "secret",
    }) as unknown as TestAdapter;

    const createdAtIso = "2026-03-18T18:33:46.451Z";
    const row = await adapter.create({
      model: "verification",
      data: { userId: "u_1", createdAt: createdAtIso },
    });

    // Insert payload should be snake_case and have Date instance.
    const insertCall = builder.calls.find((c) => c.method === "insert");
    expect(insertCall).toBeTruthy();
    const inserted = (insertCall!.args?.[0] ?? {}) as Record<string, unknown>;
    expect(Object.keys(inserted)).toEqual(expect.arrayContaining(["user_id", "created_at"]));
    expect(inserted.user_id).toBe("u_1");
    expect(inserted.created_at).toBeInstanceOf(Date);
    expect((inserted.created_at as Date).toISOString()).toBe(createdAtIso);

    // Returned row should be mapped back to camelCase for Better Auth.
    // Note: our mock returns `created_at` as string; adapter should still rename it.
    expect(row).toEqual({
      id: "m_1",
      userId: "u_1",
      createdAt: createdAtIso,
    });
  });

  it("maps where/select fields from camelCase to snake_case", async () => {
    const builder = createBuilder({
      data: [{ id: "m_1", user_id: "u_1" }],
      error: null,
    });
    const from = vi.fn(() => builder);
    createClient.mockReturnValue({ from });

    const adapter = athenaAdapter({
      url: "https://athena-db.com",
      apiKey: "secret",
    }) as unknown as TestAdapter;

    const found = await adapter.findOne({
      model: "members",
      select: ["id", "userId"],
      where: [{ field: "userId", operator: "eq", value: "u_1" }],
    });

    expect(found).toEqual({ id: "m_1", userId: "u_1" });
    expect(builder.calls).toEqual(
      expect.arrayContaining([
        { method: "select", args: ["id, user_id"] },
        { method: "eq", args: ["user_id", "u_1"] },
      ]),
    );
  });
});

