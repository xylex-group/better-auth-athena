import {
  createServer,
  type IncomingMessage,
  type ServerResponse,
} from "node:http";
import { beforeAll, afterAll, describe, expect, it, vi } from "vitest";

// `bun test` doesn't provide `vi.hoisted`, but Vitest does.
// Provide a tiny shim so the same tests can run under both runners.
if (typeof (vi as any).hoisted !== "function") {
  (vi as any).hoisted = (fn: () => unknown) => fn();
}

const { createAdapterFactory } = vi.hoisted(() => {
  return {
    createAdapterFactory: vi.fn((options: { adapter: () => unknown }) =>
      options.adapter(),
    ),
  };
});

vi.mock("better-auth/adapters", () => ({
  createAdapterFactory,
}));

let athenaAdapter: (typeof import("../src/index"))["athenaAdapter"] | null =
  null;

function readJsonBody(req: IncomingMessage): Promise<unknown> {
  return new Promise((resolve, reject) => {
    let raw = "";
    req.on("data", (chunk) => {
      raw += String(chunk);
    });
    req.on("end", () => {
      if (!raw) return resolve({});
      try {
        resolve(JSON.parse(raw));
      } catch (e) {
        reject(e);
      }
    });
    req.on("error", reject);
  });
}

const isBun = typeof (globalThis as any).Bun !== "undefined";

// This test is a true HTTP contract check, but Bun's module-mocking behavior
// differs from Vitest and can cause cross-file mock leakage.
// We run it under Vitest (pnpm test), and skip under Bun.
const contractDescribe = isBun ? describe.skip : describe;

contractDescribe("athenaAdapter (gateway contract e2e)", () => {
  let server: ReturnType<typeof createServer> | null = null;
  let baseUrl = "";
  let lastUpdatePayload: any = null;
  let updateCalls = 0;

  beforeAll(async () => {
    // Load the adapter after unmocking so we always use the real athena client.
    const mod = await import("../src/index");
    athenaAdapter = mod.athenaAdapter;

    server = createServer(async (req: IncomingMessage, res: ServerResponse) => {
      try {
        if (!req.url) {
          res.statusCode = 404;
          res.end();
          return;
        }

        if (req.method === "POST" && req.url === "/gateway/update") {
          updateCalls += 1;
          const payload = await readJsonBody(req);
          lastUpdatePayload = payload;

          res.statusCode = 200;
          res.setHeader("content-type", "application/json");
          res.end(JSON.stringify({ data: [{ id: "row_1" }] }));
          return;
        }

        res.statusCode = 404;
        res.end();
      } catch {
        res.statusCode = 500;
        res.end();
      }
    });

    await new Promise<void>((resolve) => {
      server!.listen(0, "127.0.0.1", () => resolve());
    });

    const address = server!.address();
    if (!address || typeof address === "string")
      throw new Error("Unexpected server address");
    baseUrl = `http://127.0.0.1:${address.port}`;
  });

  afterAll(async () => {
    await new Promise<void>((resolve) => server?.close(() => resolve()));
  });

  it("updateMany sends update_body: { set: ... } over /gateway/update", async () => {
    if (!athenaAdapter) throw new Error("athenaAdapter failed to load");
    const adapter = athenaAdapter({
      url: baseUrl,
      apiKey: "test-key",
      client: "test-client",
    }) as unknown as {
      updateMany: (args: {
        model: string;
        where: Array<{ field: string; operator: string; value: unknown }>;
        update: Record<string, unknown>;
      }) => Promise<number>;
    };

    const updatedCount = await adapter.updateMany({
      model: "account",
      where: [{ field: "id", operator: "eq", value: "row_1" }],
      update: { userId: "u_1" },
    });

    expect(updatedCount).toBe(1);
    expect(updateCalls).toBe(1);
    expect(lastUpdatePayload).toBeTruthy();

    // Athena client payload shape:
    // { table_name, update_body, conditions, columns, ... }
    expect(lastUpdatePayload.table_name).toBe("account");
    expect(lastUpdatePayload.update_body).toEqual({
      set: { user_id: "u_1" },
    });
  });
});
