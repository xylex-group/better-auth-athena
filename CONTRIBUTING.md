# Contributing

Thanks for taking the time to contribute to **better-auth-athena**!

## Development Setup

1. Fork and clone the repository.
2. Install dependencies:
   ```bash
   npm install
   ```
3. Run the checks:
   ```bash
   npm run typecheck
   npm run test
   npm run build
   ```

## Real database e2e tests

Integration tests run every adapter method (create, update, updateMany, delete, deleteMany, findOne, findMany, count) against a live Athena gateway and database. They are **skipped** unless `ATHENA_URL` and `ATHENA_API_KEY` are set.

To run them:

1. Create the test table (run the SQL in `tests/fixtures/athena_adapter_e2e.sql`) on the database your Athena gateway uses. The tests use client `athena-logging` and table `athena_adapter_e2e`.
2. Set environment variables:
   ```bash
   export ATHENA_URL="https://mirror3.athena-db.com"
   export ATHENA_API_KEY="x"
   ```
3. Run the real e2e suite:
   ```bash
   pnpm test -- athenaAdapter.real.e2e
   ```

## Pull Requests

- Keep changes focused and scoped to a single issue.
- Add or update tests when you change behavior.
- Ensure `npm run typecheck`, `npm run test`, and `npm run build` succeed before requesting review.
- Describe the motivation and context in the PR description.

## Reporting Issues

If you find a bug, please open an issue with clear reproduction steps, expected behavior, and actual behavior.
