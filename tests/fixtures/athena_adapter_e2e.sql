-- Active: 1772601349048@@metro.proxy.rlwy.net@13239@railway
-- Table for real-database integration tests (athenaAdapter.real.e2e.test.ts).
-- Run this against your Postgres (or via Athena gateway) before running real e2e tests.
--
-- Prerequisites: ATHENA_URL and ATHENA_API_KEY set; gateway points at a DB with this table.

CREATE TABLE IF NOT EXISTS athena_adapter_e2e (
  id TEXT PRIMARY KEY,
  name TEXT,
  email TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);
