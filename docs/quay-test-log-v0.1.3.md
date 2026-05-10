# Quay v0.1.3 worker-pipeline test log

Successor to `docs/quay-test-log-v0.1.0.md`. Captures the v0.1.3 test round
on krustentier. v0.1.3 is the first release that ships:

- Per-repo + deployment-level namespaced tag vocabulary (AST-87 + slices
  AST-90/91/92).
- Per-attempt observability (AST-93 umbrella): `agent_identity`,
  `exit_code`/`exit_signal`, `usage`/`tool_trace` artifacts,
  `events.event_data`, `attempts.diff_summary`.
- Silent-worker-exit fix: env-forwarded tmux + agent exit-status capture
  (AST-100).

It also exercises hermes-agent's bash → Python installer kickoff
(ITRY-1346 + ITRY-1350) and the per-repo tag vocab plumbing (ITRY-1336).

## Pre-test baselines

(captured during step 6 of the v0.1.3 round; populated before compaction.)

- Quay binary version (`quay --version`):
- Bun version at `/usr/local/bin/bun`:
- Claude version (`claude --version`):
- `attempts` row count baseline:
- `quay tags list --repo test-factory-code` output:
- `quay tags get-deployment` output:
- `setup-hermes.sh --verify` drift output:

## Test journey

(numbered entries, added as the test runs.)

## Findings & follow-ups

(Linear tickets filed during the run.)

## End-of-test summary

| Stage | Status | Notes |
|---|---|---|
| v0.1.3 binary install | | |
| Tag vocab reconciliation | | |
| Slack ticket creation (vocab-aware) | | |
| Enqueue + validate | | |
| Worker spawn (AST-100 fix) | | |
| `attempts.agent_identity` populated | | |
| `attempts.exit_code` / `exit_signal` populated | | |
| `usage` artifact captured | | |
| `tool_trace` artifact captured | | |
| `events.event_data` populated | | |
| `attempts.diff_summary` populated | | |
| PR opened end-to-end | | |
