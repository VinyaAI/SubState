# Contributing to SubState

Thanks for helping. This project is early (`0.1.0-alpha`); small, focused
changes are easier to review than large redesigns.

## Development setup

1. Install a recent Rust toolchain (`rust-toolchain.toml` pins the channel).
2. Clone the repo and run tests from the workspace root:

```bash
cargo test
```

3. Optional TypeScript client:

```bash
cd clients/typescript && npm install && npm run build
```

## Before you open a PR

- Run `cargo test` (and the TS client build if you touched `clients/typescript`).
- If you changed serve / boot / WS / ingest / Postgres follow, also run
  `./scripts/smoke.sh` (needs Docker + Node 20+).
- Keep diffs scoped: one concern per PR when practical.
- Do not commit `.env`, secrets, or customer data.
- Match existing code style; prefer clarifying names over comments.

## What belongs here

Good fits: engine correctness, schema/adapters, docs, tests, and local UX
(`init` / `serve` / `shell`, TS client, examples).

Out of scope for this OSS tree: hosted control plane, SSO/SAML/SCIM, SOC 2 /
private link / CMEK, and 24/7 support offerings.

## License

By contributing, you agree that your contributions are licensed under the
Apache License 2.0 (see [LICENSE](LICENSE)).
