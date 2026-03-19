import fs from "node:fs";
import path from "node:path";
import YAML from "yaml";

export type AthenaGlobalConfig = {
  athena: {
    url: string;
    apiKey: string;
    client?: string;
  };
};

// Defaults written to `config.yaml` if it doesn't exist.
// These values are intentionally placeholders; the adapter will throw if
// `url`/`apiKey` are still unset when used.
export const defaultAthenaGlobalConfig: AthenaGlobalConfig = {
  athena: {
    url: "https://mirror2.athena-db.com",
    apiKey: "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJyb2xlIjoiYXV0aGVudGljYXRlZCIsImVtYWlsIjoiZmxvcmlzQHh5bGV4LmFpIiwiZXhwIjoyNDk3MDMzNjY2fQ.LdPqTGaFq5pTokW1DA81WFjmG4nReJCOSKr3mFtXNoA",
    client: "athena_logging",
  },
};

export const DEFAULT_CONFIG_FILENAME = "config.yaml";

function resolveConfigPath(configPath?: string): string {
  if (configPath) return path.resolve(configPath);
  return path.resolve(process.cwd(), DEFAULT_CONFIG_FILENAME);
}

let cached: AthenaGlobalConfig | null = null;
let cachedConfigPath: string | null = null;
let version = 0;

let watcher: fs.FSWatcher | null = null;

function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function deepMerge<T extends Record<string, unknown>>(
  base: T,
  partial: unknown,
): T {
  if (!isObject(partial)) return base;
  const out: Record<string, unknown> = { ...base };
  for (const [k, v] of Object.entries(partial)) {
    if (v && isObject(v) && isObject(out[k])) {
      out[k] = deepMerge(out[k] as Record<string, unknown>, v);
    } else {
      out[k] = v;
    }
  }
  return out as T;
}

function ensureConfigFile(configPath: string): void {
  const dir = path.dirname(configPath);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });

  if (!fs.existsSync(configPath)) {
    const yaml = YAML.stringify(defaultAthenaGlobalConfig);
    fs.writeFileSync(configPath, yaml, "utf-8");
  }
}

function readConfigFromDisk(configPath: string): AthenaGlobalConfig {
  ensureConfigFile(configPath);
  const raw = fs.readFileSync(configPath, "utf-8");
  const parsed = YAML.parse(raw) as unknown;
  return deepMerge(defaultAthenaGlobalConfig, parsed);
}

function startWatcher(configPath: string): void {
  // Avoid multiple watchers when multiple adapter instances are created.
  if (cachedConfigPath !== null && cachedConfigPath !== configPath && watcher) {
    try {
      watcher.close();
    } catch {
      // ignore
    }
    watcher = null;
  }
  if (watcher || cachedConfigPath === configPath) return;

  try {
    watcher = fs.watch(configPath, { persistent: false }, (event) => {
      if (event !== "change" && event !== "rename") return;
      try {
        cached = readConfigFromDisk(configPath);
        version += 1;
      } catch {
        // Keep last known good config if reload fails.
      }
    });
    cachedConfigPath = configPath;
  } catch {
    // If watching isn't supported in the environment, just run without it.
  }
}

export function getAthenaGlobalConfig(options?: {
  configPath?: string;
  watch?: boolean;
}): { config: AthenaGlobalConfig; version: number } {
  const configPath = resolveConfigPath(options?.configPath);
  const shouldWatch = options?.watch ?? true;

  if (!cached || cachedConfigPath !== configPath) {
    cached = readConfigFromDisk(configPath);
    cachedConfigPath = configPath;
    version += 1;
  }

  if (shouldWatch) startWatcher(configPath);

  return { config: cached, version };
}
