# Security Policy

## Supported versions

SubState is at **0.1.0-alpha**. Only the latest commit on the default branch
receives security fixes. There are no long-term support releases yet.

## Reporting a vulnerability

Please **do not** open a public GitHub issue for security reports.

Email **security@vinya.ai** (or open a private GitHub security advisory on this
repository if that is enabled) with:

- A description of the issue and its impact
- Steps to reproduce, or a proof of concept
- Affected commit / tag if known

We will acknowledge receipt within a few business days and work with you on a
fix before any public disclosure.

## Hardening notes for operators

- Set `SUBSTATE_API_KEY` in every non-local deployment. When it is set, all
  `/v1/*` routes require `Authorization: Bearer <key>` or `x-api-key`.
- Leave `/health` and (by default) `/metrics` reachable only on trusted
  networks; bind with `BIND_ADDR` accordingly.
- Do not commit `.env`, real API keys, or customer schema dumps.
- Treat SubState as a sidecar with the same trust boundary as the data sources
  it reads. It is not a multi-tenant control plane.
