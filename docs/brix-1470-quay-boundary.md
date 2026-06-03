# BRIX-1470: Hermes / Quay Ownership Boundary

Hermes owns the local wiring needed to install and launch Quay safely. Quay owns
product/runtime behavior defaults and the contract for Quay-specific config.

## Contract

Hermes-owned settings are limited to:

- downloading and verifying the pinned Quay and worker CLI binaries;
- creating the local Quay data directory, repo mirrors, wrapper, profile.d
  export, and systemd units;
- binding `quay serve` to loopback and wiring the Hermes proxy/auth handoff;
- setting environment variable names and files needed to pass secrets without
  exposing `QUAY_ADMIN_TOKEN` to the browser;
- pointing Quay at Hermes-maintained read-only reference repo mirrors.

Quay-owned settings include:

- task lifecycle behavior, retry/staleness/concurrency defaults, and review
  gates;
- adapter product behavior after secrets are available;
- tag vocabulary semantics and validation policy;
- worker/reviewer prompt defaults and future config schema evolution.

When a setting is still present in `deploy.values.yaml` for compatibility but is
Quay-owned by this contract, treat it as transitional. Do not add new Hermes
renderer logic for that setting; move it to a Quay-owned config contract in a
separate Quay task.

## Current Inventory

| Setting | Class | Notes |
| --- | --- | --- |
| `quay.version` | Keep in Hermes | Release pin and SHA verification boundary. |
| `quay.release_repo` | Keep in Hermes | Download location for the pinned Quay binary. |
| `quay.codex` | Keep in Hermes | Root-managed Codex CLI binary pin. Used when Atlas or Quay is enabled. |
| `quay.runtime_managers` | Keep in Hermes | Ensures package managers required by registered repos exist on PATH. |
| `quay.agent_invocation` | Move to Quay | No longer sourced from `deploy.values.yaml`; existing deployed `config.toml` values are preserved and Quay defaults apply on fresh installs. |
| `quay.agents` | Move to Quay | Same as `quay.agent_invocation`; setup provisions the standard local agent CLI set for Quay-enabled hosts. |
| `quay.adapters.*.*_env` | Keep in Hermes | Secret env-var names are wiring. |
| `quay.adapters.*.enabled` | Transitional | Still rendered where needed for existing Linear/Slack flows; Quay should own adapter behavior defaults. |
| `quay.orchestrator` | Keep in Hermes | Hermes sidecar config, rendered to `quay/orchestrator.json`, not Quay `config.toml`. |
| `quay.reviewer` | Move to Quay | No longer rendered by `setup-hermes.sh`; new installs use Quay defaults. Existing deployed `config.toml` values remain until operators or Quay migration tooling change them. |
| `quay.tag_namespaces` | Move to Quay | No longer sourced, validated, or reconciled by Hermes setup. |
| `repos[].quay.package_manager` | Keep in Hermes | Launch prerequisite for worktree bootstrap. |
| `repos[].quay.install_cmd` | Transitional | Required by current Quay repo registration payload. Quay should own longer-term repo bootstrap contracts. |
| `repos[].quay.tags` | Transitional | Still reconciled per repo while repo onboarding metadata lives in deploy values. |

## Verification

Before deploy:

```bash
scripts/run_tests.sh tests/installer/test_values_helper.py tests/hermes_cli/test_setup_hermes_script.py
python3 installer/values_helper.py --values deploy.values.yaml render-quay-config --out /tmp/quay-config.toml --enable-admin-auth --reference-repos-root /tmp/hermes-code
```

For a fresh output path, the rendered config should contain launch/auth/context
wiring such as `[admin]`, `[context]`, and adapter env names. It should not
contain `agent_invocation`, `[agents]`, or `[reviewer]`. On an existing
deployment, `setup-hermes.sh` does not re-render `quay/config.toml`; it leaves
the whole file untouched so Quay/operator-managed runtime behavior persists
across installer reruns.

After deploy:

```bash
sudo systemctl status quay-serve.service
sudo /usr/local/bin/quay-as-hermes repo list
sudo /usr/local/bin/quay-as-hermes serve --help >/dev/null
```

Rollback is a values/config rollback: restore the previous
`deploy.values.yaml`, re-run `installer/setup-hermes.sh`, and, if needed,
restore the previous `<HERMES_HOME>/quay/config.toml` from host backup. Do not
weaken the loopback bind or expose `QUAY_ADMIN_TOKEN` to the browser.
