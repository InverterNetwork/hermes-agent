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

Captured 2026-05-10 19:30Z, post-reinstall on krustentier with hermes-agent
HEAD `27e7563e` (PR #49 systemd verify fix merged) and quay `v0.1.3` released
at sha `f412e08`.

- **`quay --version`** — `0.1.0+f412e08`. The `0.1.0` prefix is `package.json`'s
  `version` field, which never got bumped on release; sha `f412e08` correctly
  identifies v0.1.3. AST-81 (Low, Backlog) tracks the leading-`v` fix; the
  major.minor mismatch is a separate facet to fold in. **Treat as known
  drift.** `--verify` flags this as the only remaining drift on the host.
- **`bun --version`** — `1.3.9` at `/usr/local/bin/bun`. Reconciled by ITRY-1346's
  Python runtime-manager; the previously-ad-hoc install became a managed pin
  with no replacement (no-op since version matches the pin).
- **`claude --version`** — `2.1.132 (Claude Code)`.
- **`quay-as-hermes tags get-deployment`**:
  ```json
  {"scope":"deployment","namespaces":{"task":{"values":["bugfix","chore","config","dep-upgrade","new-feature","refactor"],"required":true}}}
  ```
- **`quay-as-hermes repo get-tags test-factory-code`**:
  ```json
  {"repo_id":"test-factory-code","namespaces":{"area":{"values":["copy","dependency","script-output"],"required":false}}}
  ```
- **`quay-as-hermes tags list --repo test-factory-code` (merged)**:
  ```json
  {"repo_id":"test-factory-code","namespaces":{"area":{"values":[...],"required":false},"task":{"values":[...],"required":true}},"enforced":true}
  ```
  `enforced: true` confirms the validator will reject tickets missing a `task-*`
  tag (deployment-required) and reject tags outside the merged vocab.
- **DB row counts** (carry-over from v0.1.0 round):
  - `tasks`: 1 (the cancelled `a1c4e61f` for ITRY-1327; budget_exhausted)
  - `attempts`: 5 (all from the cancelled task)
  - `events`: 12
  - `artifacts`: 12
  - `task_tags`: 2
  - `tag_namespaces`: 9 (`task` namespace + 6 values + `area` + 3 values, deployment+repo split)
- **`--verify`**: 61 checks, 1 known drift (AST-81 quay version string).
- **`/srv/hermes/repos/hermes-agent` HEAD**: `27e7563e`.
- **`RUNTIME_VERSION` on host**: `27e7563e5e2e0a76544e2a00d32a1cb193b7152b`.

## Findings during install

- ITRY-1336 namespace charset: my first-pass values used `task-type` as the
  namespace name, which violates quay's `[a-z0-9]+` namespace charset (no
  dashes — the validator splits ticket tags on the first dash to recover
  namespace and value). Renamed to `task` so ticket tags parse correctly
  (`task-bugfix`, not the broken `task-type-bugfix`). Caught by CI on PR #48
  before reaching the host.
- ITRY-1350 systemd verify regression: the Python port batched three
  `systemctl show -p ...` calls into one and assumed `-p` flag order was
  preserved. systemctl emits properties alphabetically, so every healthy
  timer reported as `[DRIFT]` with field labels and values swapped. Fixed
  by parsing `KEY=VALUE` lines and looking up by key (PR #49). Filed as a
  followup commit on the same release; would have been caught earlier by
  an integration test against a real systemctl.

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
