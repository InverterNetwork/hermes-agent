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
