# Fork notes

This repo is a fork of [`nousresearch/hermes-agent`](https://github.com/nousresearch/hermes-agent) used as the PR-gated rails for a self-hosted Hermes deployment.

## Layout

- Upstream source — at repo root (unchanged from upstream so `git merge upstream/main` is a clean fast-forward).
- `SOUL.md` — customized persona file overlaid on top of upstream.
- `hooks/` — overlay slot for deployment-specific hooks (currently empty).
- `installer/` — `setup-hermes.sh` and supporting scripts (filled by the installer workstream).
- `ops/` — launchd plists / systemd units, sync scripts (filled by the auto-commit and upstream-sync workstreams).

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

```sh
sudo ./installer/setup-hermes.sh \
  --fork  /srv/hermes/repos/hermes-agent \
  --state-url https://github.com/InverterNetwork/hermes-state.git \
  --user  hermes \
  --auth-method app \
  --app-id <APP_ID> \
  --app-installation-id <INSTALLATION_ID> \
  --app-key-path /root/<slug>.pem
```

The installer copies the PEM into `$TARGET/auth/github-app.pem` (root:hermes 0640), persists the App + Installation IDs to `$TARGET/auth/github-app.env`, and configures a git credential helper inside `state/.git/config` scoped to `https://github.com`. Subsequent runs reuse the persisted credentials and only need `--auth-method app` to re-assert wiring.

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
| `auth/` | `root:hermes` | 750 | App key + env config (only present with `--auth-method app`) |
| `auth/github-app.pem` | `root:hermes` | 640 | GitHub App private key (read-only to agent) |
| `auth/github-app.env` | `root:hermes` | 640 | App ID, installation ID, key path |

Agent writes through the symlinks land inside the `state/` working tree, where the auto-commit pipeline can pick them up.

### Re-run semantics

The installer is idempotent and **never destroys agent work in `state/` by default**. On a re-run with `state/` already present, it leaves the clone untouched and only re-applies repo-local git identity (so config drift gets corrected).

| Flag | Effect |
|---|---|
| _(default)_ | Preserve existing `state/`. Refuse to drop a populated `skills/memories/cron` real dir from a v0 install (operator must move data into `state/<name>/` first). |
| `--force-state` | **Destructive.** Remove `state/` and re-clone from `--state`. Wipes all uncommitted agent work and any commits not yet pushed back to the source. |

### Git identity

Repo-local `user.name = didier` and `user.email = didier@<hostname -s>` are configured inside `state/` on every run. Override with `--git-identity-email <addr>` (e.g. CI uses `didier@ci`).

### Out of scope (v0)

Some pieces are deliberately deferred:

- **`--verify` mode.** Drift detection without mutation is not yet implemented.
- **systemd / launchd unit installation.** No daemon supervision yet.
- **macOS branch.** v0 is Linux-only.
