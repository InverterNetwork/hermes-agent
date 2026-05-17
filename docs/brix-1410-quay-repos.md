# BRIX-1410: Quay Repo Registration

Source of truth is [`deploy.values.yaml`](../deploy.values.yaml) `repos:` (all six are quay-managed):

| repo id | remote | base branch | package manager | install cmd |
| --- | --- | --- | --- | --- |
| `brix-landing` | `https://github.com/InverterNetwork/brix-landing` | `main` | `pnpm` | `pnpm install --frozen-lockfile` |
| `iTRY-frontends` | `https://github.com/InverterNetwork/iTRY-frontends` | `master` | `bun` | `bun install` |
| `iTRY-monorepo` | `https://github.com/InverterNetwork/iTRY-monorepo` | `main` | `bun` | `bun install` |
| `brix-indexer` | `https://github.com/InverterNetwork/brix-indexer` | `dev` | `pnpm` | `pnpm install --frozen-lockfile` |
| `erpc` | `https://github.com/InverterNetwork/erpc` | `main` | `pnpm` | `pnpm install --frozen-lockfile` |
| `iTry-contracts` | `https://github.com/InverterNetwork/iTry-contracts` | `main` | `bun` | `true` |

## Verification commands

Local (pre-deploy):

```bash
./.venv/bin/python installer/values_helper.py --values deploy.values.yaml validate-schema
./.venv/bin/python installer/values_helper.py --values deploy.values.yaml list-repos --quay
```

Deployed host (post-deploy):

```bash
sudo /usr/local/bin/quay-as-hermes repo list
```

Expected: all six repo ids above are present.

## Enqueue verification / dry-run equivalent

Preferred: enqueue at least one real task per repo with `quay-as-hermes enqueue --linear-issue <ISSUE>`.

If no suitable issue exists for a repo yet, record the dry-run equivalent as:

1. `quay-as-hermes repo list` shows the repo id.
2. `installer/hermes_installer/verify.py --verify` reports the repo as registered (no `[DRIFT] quay repo <id>`).

