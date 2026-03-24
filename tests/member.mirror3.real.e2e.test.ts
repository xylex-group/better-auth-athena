/**
 * Real-e2e check for querying Better Auth's `member` model/table
 * against mirror3 using the `the-ark-of-floris` Athena client.
 *
 * Defaults:
 * - URL: https://mirror3.athena-db.com
 * - client: the-ark-of-floris
 *
 * Required env:
 * - ATHENA_MEMBER_E2E_API_KEY (preferred) or ATHENA_API_KEY
 *
 * Optional env:
 * - ATHENA_MEMBER_E2E_URL
 * - ATHENA_MEMBER_E2E_CLIENT
 * - ATHENA_MEMBER_E2E_X_USER_ID
 * - ATHENA_MEMBER_E2E_X_COMPANY_ID
 * - RUN_MEMBER_MIRROR3_E2E=true (recommended explicit opt-in for CI)
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

import { athenaAdapter } from "../src/index";

type Adapter = {
  findMany: (args: {
    model: string;
    where?: Array<{ field: string; operator: string; value: unknown }>;
    limit: number;
    offset?: number;
    select?: string[];
    sortBy?: { field: string; direction: "asc" | "desc" };
  }) => Promise<Record<string, unknown>[]>;
};

const e2eEnabled =
  (process.env.RUN_MEMBER_MIRROR3_E2E ?? "").toLowerCase() === "true";

const apiKey =
  process.env.ATHENA_MEMBER_E2E_API_KEY ?? process.env.ATHENA_API_KEY ?? "";

const hasConfig = Boolean(apiKey);

describe.skipIf(!e2eEnabled || !hasConfig)(
  "member mirror3 real e2e",
  () => {
    it("findMany can read member rows on mirror3/the-ark-of-floris", async () => {
      const url =
        process.env.ATHENA_MEMBER_E2E_URL ?? "https://mirror3.athena-db.com";
      const client =
        process.env.ATHENA_MEMBER_E2E_CLIENT ?? "the-ark-of-floris";

      const headers: Record<string, string> = {};
      const userId = process.env.ATHENA_MEMBER_E2E_X_USER_ID ?? client;
      const companyId = process.env.ATHENA_MEMBER_E2E_X_COMPANY_ID ?? userId;

      headers["X-User-Id"] = userId;
      headers["X-Company-Id"] = companyId;
      headers["X-Organization-Id"] = companyId;

      const adapter = athenaAdapter({
        url,
        apiKey,
        client,
        watchConfig: false,
        headers,
      }) as unknown as Adapter;

      await expect(
        adapter.findMany({
          model: "member",
          limit: 1,
        }),
      ).resolves.toSatisfy((rows: unknown) => Array.isArray(rows));
    }, 30_000);
  },
);
