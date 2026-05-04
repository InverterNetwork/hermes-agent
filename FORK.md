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

The installer assumes a local clone of the private `hermes-state` repo already exists on the host. Clone it before the first install:

```sh
sudo git clone git@github.com:InverterNetwork/hermes-state.git \
  /srv/hermes/repos/hermes-state
```

The installer reads the source's `origin` URL and re-applies it to the rendered clone, so the agent pushes back to GitHub rather than to the local source path.

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

- **Authenticated state-repo cloning (`--state-url`).** The installer requires a pre-existing local clone; cloning straight from GitHub depends on the agent-token design.
- **`--verify` mode.** Drift detection without mutation is not yet implemented.
- **systemd / launchd unit installation.** No daemon supervision yet.
- **macOS branch.** v0 is Linux-only.
