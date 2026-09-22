## Summary

<!-- What changed and why. -->

## Checklist

- [ ] `cargo test` passes
- [ ] `./scripts/smoke.sh` run if serve / boot / WS / ingest / Postgres follow changed (needs Docker + Node 20+)
- [ ] TypeScript client built (`cd clients/typescript && npm run build`) if `clients/typescript` changed
- [ ] No `.env`, secrets, or customer data committed
- [ ] Docs / [CHANGELOG.md](../CHANGELOG.md) updated if the public surface changed
- [ ] Diff stays scoped to one concern when practical
