import {
  createAdapterFactory,
  type AdapterFactory,
  type DBAdapterDebugLogOption,
} from "better-auth/adapters";
import type { BetterAuthOptions } from "better-auth";
import { createClient } from "@xylex-group/athena";
import { getAthenaGlobalConfig } from "./config";
import { createMethod } from "./methods/create";
import { updateMethod } from "./methods/update";
import { updateManyMethod } from "./methods/update-many";
import { deleteMethod } from "./methods/delete";
import { deleteManyMethod } from "./methods/delete-many";
import { findOneMethod } from "./methods/find-one";
import { findManyMethod } from "./methods/find-many";
import { countMethod } from "./methods/count";

/**
 * Configuration options for the Athena adapter.
 */
export interface AthenaAdapterConfig {
  /**
   * The URL of your Athena gateway.
   */
  url?: string;
  /**
   * The API key for authenticating with the Athena gateway.
   */
  apiKey?: string;
  /**
   * The client name sent in requests to the Athena gateway.
   */
  client?: string;

  /**
   * Optional override for the YAML config path.
   * Defaults to `./config.yaml` (resolved from `process.cwd()`).
   */
  configPath?: string;

  /**
   * When enabled, the adapter will reload `config.yaml` on changes.
   *
   * @default true
   */
  watchConfig?: boolean;
  /**
   * Helps you debug issues with the adapter.
   */
  debugLogs?: DBAdapterDebugLogOption;
  /**
   * If the table names in the schema are plural.
   *
   * @default false
   */
  usePlural?: boolean;

  /**
   * Optional headers sent with every request (e.g. `X-User-Id` if your gateway requires it for delete).
   */
  headers?: Record<string, string>;
}

/**
 * Create a Better-Auth database adapter backed by @xylex-group/athena.
 *
 * Column names are kept in snake_case as required by the Athena gateway.
 *
 * @example
 * ```ts
 * import { betterAuth } from "better-auth";
 * import { athenaAdapter } from "better-auth-athena";
 *
 * export const auth = betterAuth({
 *   database: athenaAdapter({
 *     url: process.env.ATHENA_URL!,
 *     apiKey: process.env.ATHENA_API_KEY!,
 *     client: "my-app",
 *   }),
 * });
 * ```
 */
export const athenaAdapter = (
  config: AthenaAdapterConfig,
): AdapterFactory<BetterAuthOptions> => {
  let dbClient: any | null = null;
  let lastDbConfigVersion = -1;

  const shouldUseFixedConfig =
    typeof config.url === "string" &&
    config.url.length > 0 &&
    typeof config.apiKey === "string" &&
    config.apiKey.length > 0;

  function ensureDbClient(): any {
    if (shouldUseFixedConfig) {
      if (!dbClient) {
        dbClient = createClient(config.url!, config.apiKey!, {
          client: config.client,
          ...(config.headers && { headers: config.headers }),
        } as any);
      }
      return dbClient;
    }

    const { config: globalConfig, version } = getAthenaGlobalConfig({
      configPath: config.configPath,
      watch: config.watchConfig ?? true,
    });

    const url = config.url ?? globalConfig.athena.url;
    const apiKey = config.apiKey ?? globalConfig.athena.apiKey;
    const client = config.client ?? globalConfig.athena.client;

    if (!url || !apiKey) {
      throw new Error(
        `[AthenaAdapter] Missing Athena connection details. Set both 'athena.url' and 'athena.apiKey' in config.yaml (or pass 'url'/'apiKey' to athenaAdapter).`,
      );
    }

    if (!dbClient || version !== lastDbConfigVersion) {
      dbClient = createClient(url, apiKey, {
        client,
        ...(config.headers && { headers: config.headers }),
      } as any);
      lastDbConfigVersion = version;
    }

    return dbClient;
  }

  function getConnectionConfig() {
    if (shouldUseFixedConfig) {
      return {
        url: config.url!,
        apiKey: config.apiKey!,
        client: config.client,
        headers: config.headers,
      };
    }
    const { config: globalConfig } = getAthenaGlobalConfig({
      configPath: config.configPath,
      watch: false,
    });
    return {
      url: config.url ?? globalConfig.athena.url,
      apiKey: config.apiKey ?? globalConfig.athena.apiKey,
      client: config.client ?? globalConfig.athena.client,
      headers: config.headers,
    };
  }

  const deps = { ensureDbClient, getConnectionConfig, headers: config.headers };

  return createAdapterFactory({
    config: {
      adapterId: "athena",
      adapterName: "Athena Adapter",
      usePlural: config.usePlural ?? false,
      debugLogs: config.debugLogs ?? false,
      supportsJSON: true,
      supportsDates: true,
      supportsBooleans: true,
      supportsNumericIds: true,
      supportsUUIDs: true,
      supportsArrays: true,
    },
    adapter: () => {
      return {
        create: createMethod(deps),
        update: updateMethod(deps),
        updateMany: updateManyMethod(deps),
        delete: deleteMethod(deps),
        deleteMany: deleteManyMethod(deps),
        findOne: findOneMethod(deps),
        findMany: findManyMethod(deps),
        count: countMethod(deps),
        options: {
          debugLogs: config.debugLogs ?? false,

        },
      };
    },
  });
};

// For testing purposes, we export the internal config function to allow resetting the cached client between tests.
export { getAthenaGlobalConfig } from "./config";
