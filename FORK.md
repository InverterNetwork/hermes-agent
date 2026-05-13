# Fork notes

This repo is a fork of [`nousresearch/hermes-agent`](https://github.com/nousresearch/hermes-agent) used as the PR-gated rails for a self-hosted Hermes deployment.

## Layout

- Upstream source — at repo root (unchanged from upstream so `git merge upstream/main` is a clean fast-forward).
- `SOUL.md` — customized persona file overlaid on top of upstream.
- `hooks/` — overlay slot for deployment-specific hooks (currently empty).
- `deploy.values.yaml` — single source of truth for org-specific values (identity, Slack manifest fields, runtime allowlist, quay deployment knobs). See [Re-forking for another org](#re-forking-for-another-org).
- `installer/` — `setup-hermes.sh`, the values helper, and the Slack manifest template.
- `ops/` — launchd plists / systemd units, sync scripts (filled by the auto-commit and upstream-sync workstreams).

## Re-forking for another org

Everything org-specific in this fork lives in `deploy.values.yaml`. To re-instantiate this fork for a different org:

1. Fork this repo (or clone + push to a new origin).
2. Edit `deploy.values.yaml` end-to-end — `org.*`, `slack.app.*`, `slack.runtime.*`, `gateway.*` (LLM provider pin and optional approval mode), `auth.github_app.*` (numeric IDs only — the PEM stays out-of-band), `linear.teams.*` (team UUIDs exposed as `LINEAR_TEAM_<KEY>`), `repos[]` (every entry produces a code mirror; entries with a `quay:` sub-block are additionally registered with quay), `quay.*` (set `quay.version` to a published `v*` tag to enable quay provisioning, or leave it empty to skip the quay binary + data dir entirely). Tokens never go here; they're staged at install time.
3. Run `installer/setup-hermes.sh` on the target host. The installer:
   - reads `deploy.values.yaml` (override the path with `--values <file>` if needed),
   - renders `installer/slack-manifest.json.tmpl` to `<HERMES_HOME>/slack-manifest.json` for paste-install into Slack's manifest UI,
   - seeds `<HERMES_HOME>/config.yaml` from `slack.runtime.*` on first install (preserved on re-runs — operator hand-edits survive),
   - merges `gateway.model_provider` / `gateway.model_base_url` into `<HERMES_HOME>/config.yaml`'s `model:` block on every run (deterministic — no longer dependent on `hermes auth add`), and merges optional `gateway.approvals_mode` into `approvals.mode` (`off` bypasses approval prompts for trusted internal Slack deployments; hardline-blocked commands still remain blocked),
   - rewrites `<HERMES_HOME>/auth/gateway-runtime.env` from `slack.runtime.allowed_users` and `linear.teams.*` on every run (`SLACK_ALLOWED_USERS`, `LINEAR_TEAM_<KEY>` — non-secret, version-controlled, no operator re-typing on rotations),
   - renders `<HERMES_HOME>/gateway-org-defaults.md` from `repos[]` + `linear.teams` (compact org-defaults seed the gateway loads into its cached system prompt — code-mirror location + per-repo Linear team mapping; reflects values.yaml, rewritten every run),
   - regenerates `<HERMES_HOME>/auth/github-app.env` from `auth.github_app.*` on every `--auth-method app` run; CLI flags (`--app-id`, `--app-installation-id`) stay as per-run overrides,
   - configures `git user.name` on agent commits to `org.agent_identity_name`.
4. Stage all runtime secrets with `stage-secrets.sh` (interactive — writes `<HERMES_HOME>/auth/slack.env`, `auth/hermes.env`, and `auth/quay.env` in one pass). Required: `SLACK_BOT_TOKEN`, `SLACK_APP_TOKEN`, plus `LINEAR_API_KEY` when `quay.version` is set. `ANTHROPIC_API_KEY` is an optional quay-side prompt, skipped on Linear-only deployments. Re-runs preserve unchanged values; identical content skips the gateway restart.

Acceptance: once `deploy.values.yaml` is set for the new org, `grep -RE 'BabyDidier|didier|C0B23MZ0USV|lmdtfy' installer/ ops/ gateway/` should return no matches outside the values file.

## Remotes

- `origin` — this fork.
- `upstream` — `https://github.com/nousresearch/hermes-agent`.

## Branch protection

`main` requires a PR with one approving review; force-push and branch deletion are disabled. Direct pushes are blocked for non-admins.

## Installation

`installer/setup-hermes.sh` renders the fork into `~/.hermes/` with an OS-level rails-vs-state permission boundary (rails root-owned read-only; state agent-owned).

### Prerequisites

Pass exactly one of:

- `--state <path>` — local clone of `hermes-state` already on the host. Clone it once before the first install:
  ```sh
  sudo git clone git@github.com:InverterNetwork/hermes-state.git \
    /srv/hermes/repos/hermes-state
  ```
  The installer reads the source's `origin` URL and re-applies it to the rendered clone, so the agent pushes back to GitHub rather than to the local source path.
- `--state-url <url>` — HTTPS URL to clone direct from GitHub. Requires `--auth-method app` (see [GitHub App auth](#github-app-auth) below) so the installer can authenticate the clone.

### GitHub App auth

For real deploys, the agent authenticates to GitHub as a dedicated App (one App per fork; install it on `hermes-agent` + `hermes-state` only). Pass `--auth-method app` to wire it in.

**One-time App provisioning** (operator, in the GitHub UI):

1. **Settings → Developer settings → GitHub Apps → New GitHub App** under the org that owns the private repos.
2. **Permissions** (repository): _Contents: Read and write_ + _Pull requests: Read and write_. Metadata stays read-only by default.
3. **Webhooks**: disable (the agent polls; no inbound webhook is needed).
4. **Install** the App on the org and **scope it** to exactly `hermes-agent` + `hermes-state` (use "Only select repositories" — never grant org-wide).
5. Generate a private key (`Settings → Generate a private key`); a `<slug>.<date>.private-key.pem` downloads.
6. Note the **App ID** (Settings page) and **Installation ID** (the integer in the post-install URL: `https://github.com/organizations/<org>/settings/installations/<id>`).
7. Stage the key on the host (e.g. `scp` it to `/root/<slug>.pem`, mode 0600 root-owned).

**Per-host install:**

Populate `auth.github_app.id` and `auth.github_app.installation_id` in `deploy.values.yaml` once (numeric identifiers — not secrets), then:

```sh
sudo ./installer/setup-hermes.sh \
  --fork  /srv/hermes/repos/hermes-agent \
  --state-url https://github.com/InverterNetwork/hermes-state.git \
  --user  hermes \
  --auth-method app \
  --app-key-path /root/<slug>.pem
```

`--app-id` / `--app-installation-id` are still accepted as per-run overrides (e.g. swap to a staging App for one install) but no longer required when the values file carries them.

The installer copies the PEM into `$TARGET/auth/github-app.pem` (root:hermes 0640), regenerates `$TARGET/auth/github-app.env` from `auth.github_app.*` on every run, and configures a git credential helper inside `state/.git/config` scoped to `https://github.com`. Subsequent runs reuse the persisted PEM and only need `--auth-method app` to re-assert wiring.

Every run with `--auth-method app` ends with a live mint call (`hermes_github_token.py check`) to confirm the agent can actually authenticate to GitHub end-to-end. **This means re-installs require live `api.github.com` egress.** If GitHub is degraded or the host is offline, pass `--skip-auth-check` to skip the mint and still update the rails / state symlinks.

Key rotation: re-run with `--app-key-path <new-path>` to overwrite the staged PEM. The token cache at `$TARGET/cache/github-token.json` self-refreshes within 5 minutes of expiry; delete it to force-refresh sooner.

### Render-target layout

Under `$TARGET` (default: `~hermes/.hermes/`):

| Path | Owner | Mode | Notes |
|---|---|---|---|
| `hermes-agent/` | `root:root` | 755 | rsynced from the fork; venv lives inside |
| `SOUL.md`, `RUNTIME_VERSION` | `root:root` | 644 | rails overlay |
| `hooks/` | `root:root` | 755 | rails overlay slot |
| `state/` | `hermes:hermes` | 755 | clone of `hermes-state`; `.git/` agent-owned |
| `skills`, `memories`, `cron` | `hermes:hermes` (symlink) | — | resolve to `state/<name>` |
| `sessions/`, `logs/`, `cache/` | `hermes:hermes` | 755 | local-only (gitignored content) |
| `code/` | `hermes:hermes` | 755 | code mirrors root. One subdir per `repos[]` entry, each a working-tree clone refreshed by `hermes-code-sync` (5-min cadence). Read by the gateway when answering codebase questions in Slack. |
| `quay/` | `hermes:hermes` | 755 | quay data dir (sqlite, worktrees, bare clones, logs); seeded `config.toml` lives inside, preserved across re-runs. Only present when `quay.version` is set in `deploy.values.yaml` |
| `auth/` | `root:hermes` | 750 | Staged secrets (`slack.env`, `hermes.env`, `quay.env`) and values-derived runtime config. Always present (created on every install). |
| `auth/github-app.pem` | `root:hermes` | 640 | GitHub App private key (read-only to agent). Only with `--auth-method app`. |
| `auth/github-app.env` | `root:hermes` | 640 | App ID, installation ID, key path. Regenerated from `auth.github_app.*` on every run. |
| `auth/gateway-runtime.env` | `root:hermes` | 640 | Non-secret env vars from `deploy.values.yaml` (`SLACK_ALLOWED_USERS`, `LINEAR_TEAM_<KEY>`, …). Rewritten every run; do not hand-edit. |
| `gateway-org-defaults.md` | `root:hermes` | 640 | Compact deployment-defaults seed loaded by the gateway agent into its cached system prompt: code-mirror location + per-repo Linear team mapping (`issue_tracker.linear.team` on each `repos[]` entry). Rendered from `deploy.values.yaml` on every run; do not hand-edit. |

Agent writes through the symlinks land inside the `state/` working tree, where the auto-commit pipeline can pick them up.

### Re-run semantics

The installer is idempotent and **never destroys agent work in `state/` by default**. On a re-run with `state/` already present, it leaves the clone untouched and only re-applies repo-local git identity (so config drift gets corrected).

| Flag | Effect |
|---|---|
| _(default)_ | Preserve existing `state/`. Refuse to drop a populated `skills/memories/cron` real dir from a v0 install (operator must move data into `state/<name>/` first). |
| `--force-state` | **Destructive.** Remove `state/` and re-clone from `--state`. Wipes all uncommitted agent work and any commits not yet pushed back to the source. |

### Git identity

Repo-local `user.name = didier` and `user.email = didier@<hostname -s>` are configured inside `state/` on every run. Override with `--git-identity-email <addr>` (e.g. CI uses `didier@ci`).

### `--verify` mode

Read-only health check. Does not write to the install:

```sh
bash installer/setup-hermes.sh --verify \
  --fork /srv/hermes/repos/hermes-agent \
  --target /home/hermes/.hermes \
  --user hermes \
  --auth-method app
```

Reports `[OK] <subject>` per check (suppress with `--quiet`) and `[DRIFT] <subject>: <detail>` to stderr for each failure, with a closing `==> verify: N checks, M drift`. Exits 0 if no drift, 1 otherwise. Each check is independent so a single run surfaces every drifted subject.

### Out of scope (v0)

Some pieces are deliberately deferred:

- **launchd unit installation.** systemd timers ship today; the macOS launchd path is tracked under the existing macOS TODO at the top of `installer/setup-hermes.sh`.
- **macOS branch.** v0 is Linux-only.
