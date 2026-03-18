# better-auth-athena

current version: `1.0.1`

A Better-Auth database adapter for the `@xylex-group/athena` gateway. It lets Better-Auth read and write data through Athena while keeping column names in `snake_case` as required by the gateway.

## Installation

```bash
npm install better-auth-athena
```

This package relies on the following peer dependencies, so ensure they are installed in your project:

```bash
npm install better-auth @xylex-group/athena
```

## Usage

```ts
import { betterAuth } from "better-auth";
import { athenaAdapter } from "better-auth-athena";

export const auth = betterAuth({
  database: athenaAdapter({
    url: process.env.ATHENA_URL!,
    apiKey: process.env.ATHENA_API_KEY!,
    client: "my-app",
  }),
});
```

## Configuration

`athenaAdapter` accepts the following options:

| Option | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `url` | `string` | ✅ | — | Athena gateway URL. |
| `apiKey` | `string` | ✅ | — | API key used to authenticate with Athena. |
| `client` | `string` | ❌ | — | Client name included with gateway requests. |
| `debugLogs` | `DBAdapterDebugLogOption` | ❌ | `false` | Enables Better-Auth adapter debug logs. |
| `usePlural` | `boolean` | ❌ | `false` | Treats table names as plural when mapping models. |

## Notes

- `findMany` sorting is performed in memory because the Athena SDK does not expose an order-by method on its query builder.
- The adapter enables JSON, date, boolean, and numeric ID support in Better-Auth.

## Development

Node.js 20.19.0 or later is required for the test/build tooling.

```bash
npm run typecheck
npm run test
npm run build
```

## CI/CD

GitHub Actions runs the typecheck, test, and build steps for every pull request and push. Releases can be published to npm by creating a GitHub release after the CI workflow succeeds.

## Contributing

See [CONTRIBUTING.md](./CONTRIBUTING.md) for setup steps and the contribution process.

## Contributors

See [CONTRIBUTORS.md](./CONTRIBUTORS.md) for the current list of project contributors.

## License

MIT
