# quay v0.1.0 worker-pipeline test log — krustentier (2026-05-08)

Living log of every problem, warning, or weirdness observed during the first
end-to-end exercise of `quay`'s task pipeline against `ITRY-1327` on krustentier.
Each entry has a **Locus of fix** field — `hermes-agent`, `quay`, `host-local`,
or `TBD`. After the test we triage the open items and turn each into the right
shape of follow-up (PR on either side, ticket, runbook entry).

Status legend: ✅ resolved during this session, 🟡 open, 🔵 informational.

Test fixture:
- Linear ticket: ITRY-1327 ("Update test factory script copy to 'Ran ans werk!'")
- Repo: `InverterNetwork/test-factory-code`
- Krustentier: hermes-agent at `81c87e98` (PRs #33/#34/#35 installed today)
- Quay slack adapter: DISABLED — task progress visible via journalctl + tmux only

---

## 1. 🟡 `sudo -u hermes quay <cmd>` silently picks up the wrong config [ITRY-1334]

- **Symptom:** First enqueue attempt failed with
  `{"error":"adapter_not_enabled","message":"[adapters.linear].enabled = false","adapter":"linear"}`
  even though `/home/hermes/.hermes/quay/config.toml` clearly contains
  `[adapters.linear]\nenabled = true`.
- **Root cause:** Quay's config-file resolution order is (`src/cli/config.ts:10`):
  1. `--config <path>`
  2. `$QUAY_CONFIG_DIR/config.toml`
  3. `$QUAY_DATA_DIR/config.toml`
  4. `~/.quay/config.toml`

  The systemd unit (`ops/quay-tick.service`) sets `QUAY_DATA_DIR=__TARGET_DIR__/quay`,
  so the tick path always finds `[adapters.linear].enabled = true` and is fine.
  But `sudo -u hermes /usr/local/bin/quay …` strips env by default → no
  `QUAY_DATA_DIR`, no `QUAY_CONFIG_DIR` → falls back to `~hermes/.quay/config.toml`,
  which doesn't exist → empty config → `linearEnabled = false` → fail-closed.
- **Compounding factor:** A stale `~hermes/.quay/` directory exists on krustentier
  (timestamps from 2026-05-07 09:01 — pre-dates the systemd unit becoming
  authoritative for `QUAY_DATA_DIR`). Previous ad-hoc commands wrote a SQLite
  there. Today's `quay repo list` health check ran against THIS db, not the
  systemd-unit db at `~hermes/.hermes/quay/quay.db`. Every CLI invocation has
  been silently bifurcated between two data dirs, and nobody noticed because
  `repo list` happens to show the same `test-factory-code` row in both (added
  during the same install pass). The two SQLite files are diverging quietly.
- **Tried:** `--config /home/hermes/.hermes/quay/config.toml` flag — rejected
  as `{"error":"usage_error","message":"unknown command: --config"}`. The
  config-source-1 entry in the resolution comment is aspirational; there's no
  CLI flag wiring for it.
- **Locus of fix:** **TBD** — multiple cheap fixes possible, want to discuss before deciding:
  1. **hermes-agent:** stage-secrets.sh / setup-hermes.sh could drop a
     `~hermes/.bashrc.d/quay.sh` (or a wrapper script `/usr/local/bin/quay-as-hermes`)
     that exports `QUAY_DATA_DIR` before invoking the binary, so ad-hoc
     `sudo -u hermes quay …` works without ceremony.
  2. **quay:** wire the `--config` flag the comment promises (cheap, one-arg
     handler in the CLI dispatcher).
  3. **quay:** make `~/.quay/config.toml` not the silent fallback — fail-loud
     when no config file is found at any of the higher-priority paths, so the
     bifurcation can't happen silently.
- **Workaround used in this session:** Run with explicit env:
  `sudo -u hermes env QUAY_DATA_DIR=/home/hermes/.hermes/quay /usr/local/bin/quay enqueue …`
  (verify this actually works once the stale `~hermes/.quay/` is moved aside —
  the second attempt today STILL failed even with `env QUAY_DATA_DIR=…`, which
  needs further diagnosis: maybe `sudo` was stripping it before `env` saw it,
  or the resolution didn't actually reach the QUAY_DATA_DIR rung.)
- **Why this matters beyond test mechanics:** Operators following the runbook
  ("ssh in, run `sudo -u hermes quay repo list`") get a phantom-coherent shell
  view that drifts from what the tick supervisor actually runs against. Any
  manual `quay repo add`/`repo remove` issued ad-hoc lands in `~/.quay/` and
  the tick never sees it.

## 2. 🟡 Linear adapter spawns `process.execPath -e <script>` — fatal in compiled binary [AST-85]

- **Symptom:** With config + env wired correctly, enqueue fails:
  ```
  {"error":"adapter_error",
   "message":"Linear getIssue failed: Linear request child process failed
              (exit 1): {\"error\":\"usage_error\",
                          \"message\":\"unknown command: -e\",\"command\":\"-e\"}",
   "adapter":"linear","retryable":false}
  ```
  The child-process output is itself a quay `usage_error` envelope — i.e. the
  child is **another instance of the quay binary**, not bun.
- **Root cause:** `src/adapters/linear.ts:273-274` spawns the fetch transport
  with `cmd: [process.execPath, "-e", linearFetchScript()]`. The intent is
  "re-invoke bun with `-e <inline JS>` to make the HTTPS call in a fresh
  process so the token never lands on argv." That works in dev (`bun run`)
  where `process.execPath` is `/usr/bin/bun`. Once shipped via
  `bun build --compile`, `process.execPath` resolves to the **compiled binary
  itself** (`/usr/local/bin/quay`), so the spawn becomes
  `/usr/local/bin/quay -e <script>`. Quay's CLI dispatcher reads `-e` as a
  subcommand and rejects it.
- **Blast radius:** Every Linear API call is broken in v0.1.0's shipped
  binary — `enqueue --linear-issue`, ticket-context fetch during ticks, the
  adapter-health probe, anything that touches `linear.getIssue`. The Slack
  adapter uses the same `process.execPath -e` trick (per the comment at
  `linear.ts:268-269`: *"same reasons as SlackAdapter"*) so it's almost
  certainly broken the same way; gated off in our deployment so we haven't
  observed it.
- **Why latent until now:** Every test path so far ran via `bun run` /
  `bun test` (where `process.execPath = bun`). The compiled binary was smoke-
  tested for `quay tick` against an empty queue (no Linear call) and `quay
  repo list/add/remove` (no Linear call). PR #16-#23 install plumbing never
  exercised an end-to-end Linear fetch through the shipped binary — this is
  the first session that did.
- **Locus of fix:** **quay.** Two viable shapes:
  1. Resolve a bun runtime path at startup (e.g. `Bun.which("bun")` or accept
     `QUAY_BUN_PATH`) and spawn that, falling back to in-process `fetch` when
     no bun binary is reachable. Preserves the argv-sanitisation property.
  2. Drop the spawn entirely and call `fetch` in-process. The original
     justification (token on argv) doesn't apply: the current code already
     passes the token via env (`QUAY_LINEAR_TOKEN` / similar), not argv. The
     spawn buys nothing and breaks compiled distribution.
  Option 2 is simpler and removes a class of bug; option 1 keeps a layer of
  isolation if there's a reason the inline fetch was wanted in a separate
  process I'm not seeing.
- **Workaround for this session:** None on the operator side. Either (a) run
  quay from `bun run src/cli/index.ts …` against a checkout of the v0.1.0
  tag (loses the deployment-shaped test), or (b) patch + rebuild + reinstall
  the binary on krustentier. Pausing the test here pending the AST-85 fix.
- **Filed as:** [AST-85](https://linear.app/aster69/issue/AST-85) (priority
  High). Recommendation in the ticket body: drop the spawn, call `fetch`
  in-process — token is already env-passed, the spawn buys nothing.
- **Open follow-ups this blocks:** the entire worker pipeline (steps 2-6 of
  this session's plan) — can't enqueue, can't observe tick claim, can't see
  worker tmux, can't review-pr. Re-test against a fixed binary once AST-85
  ships in v0.1.1 (or whatever release shape gets cut).
- **Resolved 2026-05-08:** AST-85 (PR #25) and AST-86 (PR #26) merged to
  `lafawnduh1966/quay` main; v0.1.1 cut and published. hermes-agent PR #37
  bumped `quay.version` to v0.1.1; krustentier reinstalled. Confirmed
  Linear adapter no longer spawns `-e` child — see entry #5 below for
  what broke next.

---

## 3. 🟡 Compiled binary self-reports stale `package.json` version (cosmetic, AST-81 escalation)

- **Symptom:** v0.1.1 install completed cleanly, but `quay --version` on
  krustentier reports `0.1.0+7b048ff`. The `7b048ff` SHA is correct (it's
  the v0.1.1 main HEAD), so the binary IS the v0.1.1 build. Just the
  semver prefix is wrong.
- **Root cause:** `package.json` wasn't bumped from `0.1.0` to `0.1.1`
  alongside the v0.1.1 tag. `scripts/embed.ts` emits
  `${pkg.version}+${shortSHA}`, so the binary self-identifies with the
  stale package.json semver and the new commit SHA.
- **Latent impact:**
  - Cosmetic for now — `setup-hermes.sh`'s install path doesn't strict-
    check the version (only the `--verify` path does), so the install
    succeeded.
  - **`--verify` will now flag drift on every host** because
    `setup-hermes.sh:525-529` does a substring check
    `[[ "$actual_version" == *"$pin_semver"* ]]`, with `pin_semver=0.1.1`
    and `actual_version=0.1.0+7b048ff`. False-positive drift.
- **Locus of fix:** **quay.** Already filed as **AST-81** ("Quay-side
  alignment: embed `v${pkg.version}`"). v0.1.1 worsened it from "missing
  `v` prefix" to "stale `pkg.version`". Two cheap shapes for the fix:
  1. Bump `package.json` as part of the release-cut workflow (tag-time
     check that `pkg.version` matches `${GITHUB_REF_NAME#v}`; fail the
     release if not).
  2. Have `embed.ts` derive the version from the tag (`git describe`)
     when running in CI, falling back to `pkg.version` for dev.
  Recommendation in the ticket: option 1 — explicit tag/pkg consistency,
  one source of truth.
- **Workaround:** None needed for the live install. Operators running
  `--verify` on v0.1.1 should expect a "quay binary version" drift line
  until AST-81 ships.

## 4. 🟡 `quay-as-hermes` wrapper returns empty results when invoked from a non-readable cwd [ITRY-1344, AST-89]

- **Symptom:** Post-reinstall, `/usr/local/bin/quay-as-hermes repo list`
  returned `[]`. But the canonical DB at `~hermes/.hermes/quay/quay.db`
  has the `test-factory-code` registration row (verified via systemd-run
  with the same `QUAY_DATA_DIR` — that returns the row correctly). Worse:
  invoking the wrapper also **regenerated** the stale `~hermes/.quay/`
  data dir that the install just reconciled.
- **Root cause (verified):** The wrapper at `/usr/local/bin/quay-as-hermes`
  does `exec sudo -u hermes env QUAY_DATA_DIR=… /usr/local/bin/quay "$@"`
  and inherits sudo's cwd, which after `ssh krustentier; sudo …` is
  `/root` (root's home, mode 0750, not readable by `hermes`). When quay
  starts up with `cwd=/root`:
  - The bun-compiled binary apparently can't fully initialize SQLite WAL
    state from there (the WAL file holds the data; the main `.db` is
    just the header). `repo list` returns `[]` instead of the actual rows.
  - And the binary creates a fresh `~hermes/.quay/` data dir, so the
    falls-back-on-startup behaviour that ITRY-1334's reconciler fixes
    re-fires every time the wrapper runs from `/root`.
- **Reproduction matrix:**
  | Invocation | cwd at quay startup | result |
  |---|---|---|
  | `sudo -u hermes env QUAY_DATA_DIR=… quay repo list` (from `/root`) | `/root` | `[]` (wrong) |
  | `sudo -u hermes -H env QUAY_DATA_DIR=… quay repo list` | `/root` | `[]` (wrong; `-H` only sets HOME, not cwd) |
  | `sudo -u hermes sh -c "cd /home/hermes && env QUAY_DATA_DIR=… quay repo list"` | `/home/hermes` | row present ✅ |
  | `sudo systemd-run --uid=hermes --setenv=QUAY_DATA_DIR=… quay repo list` | `/` | row present ✅ |
- **Root-cause locus:** This is **two bugs entangled**:
  1. **hermes-agent (wrapper):** trivial fix — wrapper should
     `cd "$AGENT_HOME"` (or `cd /`) before `exec`. One-line change. The
     identical pattern already exists in `setup-hermes.sh:1521` for the
     git-config-via-sudo subshell, with the same comment about CWD
     read-stat.
  2. **quay (silent fall-back-and-create on uninitialisable cwd):** even
     with `QUAY_DATA_DIR` set, when the binary can't read its cwd it
     should fail-loud or at least skip the `~/.quay/` last-resort
     creation. Silently materialising a parallel data dir is exactly
     what ITRY-1334 was supposed to prevent.
- **Severity:** Functional bug — the documented "sanctioned operator
  path" for ad-hoc invocations doesn't work as advertised when ssh'ing
  in as root and using sudo's default cwd, which is the canonical
  operator workflow on krustentier. Every operator invocation of
  `quay-as-hermes` either gives wrong data or pollutes the host with
  a regenerating stale data dir.
- **Workaround in this session:** Use `systemd-run` for any quay
  invocation that needs to read state. The wrapper is unsafe to use
  until fixed.
- **To file:** Two tickets — one ITRY for the wrapper cwd fix, one AST
  for the silent fall-back behaviour. Filing inline below.

## 5. 🟡 Validator spawn falls to `bun run` in compiled binary — `enqueue --linear-issue` blocked [AST-88]

- **Symptom:** With v0.1.1 installed (Linear adapter spawn fix from
  AST-85 in place), `quay enqueue --linear-issue ITRY-1327` now fails
  with:
  ```
  {"error":"internal_error","message":"Executable not found in $PATH: \"bun\""}
  ```
- **Root cause:** `src/core/validator_runner.ts:35-67` picks one of three
  spawn shapes:
  1. Explicit `binPath` / `bunPath` (test injection).
  2. `isCompiledBinary()` true → recurse through `process.execPath` with
     subcommand `validate-ticket` (in-process schema).
  3. Fallback → `bun run <DEFAULT_VALIDATOR_BIN>`.
  `isCompiledBinary()` is defined as `Bun.embeddedFiles?.length > 0`. In
  v0.1.1's release-built binary, `Bun.embeddedFiles` is empty (the
  release workflow `bun build --compile --target=… src/cli/index.ts` has
  no static-import side-channel that bun's compile step picks up as an
  asset to embed). So branch 3 fires and tries to spawn `bun`, which
  isn't on the host's PATH (no reason it would be — quay is a sealed
  binary distribution).
- **Same family as AST-85/86, missed in their sweep:** the comment at
  `validator_runner.ts:32-34` already names the trap — *"DEFAULT_VALIDATOR_BIN
  resolves to a virtual /$bunfs/... path that no spawned process can
  read. We must instead recurse through the running binary itself"* —
  but the gate (`isCompiledBinary()`) never trips on the production build.
- **Why latent until now:** Same as AST-85: every prior test path ran
  via `bun run`/`bun test`, where `Bun.embeddedFiles` is empty BUT bun is
  on PATH so the fallback spawn worked anyway. The first end-to-end
  exercise of `enqueue --linear-issue` against the compiled binary is
  this session, and it lands on the third branch.
- **Severity:** **Hard blocker for `quay enqueue --linear-issue`.** Same
  shipped-binary impact as AST-85: every Linear-driven enqueue fails.
- **Locus of fix:** **quay.** Two shapes:
  1. **Replace `isCompiledBinary()` with a more robust detector.** The
     check currently keys off `Bun.embeddedFiles.length > 0`, which is
     fragile; a more direct signal is `process.execPath` ending in
     `quay` (or any `--target=bun-*-*` build) vs. ending in `bun`. A
     simpler heuristic: detect once at startup whether `bun` is on PATH;
     if not, force the recurse branch. Or check
     `import.meta.url.startsWith("/$bunfs/")`, which is true in compiled
     binaries.
  2. **Force the recurse branch unconditionally for the production
     dispatcher.** Tests already inject explicit overrides, so the
     "non-compiled fallback" branch only matters for `bun run
     src/cli/index.ts` — but in that mode, recursing through
     `process.execPath` (= bun itself) with `validate-ticket` argv
     also works. Making branch 2 the default and dropping branch 3
     entirely simplifies the runner.
  Recommendation: **option 2** — same pattern AST-85 took (drop the
  fragile branch, take the always-correct one).
- **Workaround for this session:** None on the operator side. Same
  shape as AST-85: either patch + rebuild, or install `bun` on
  krustentier AND somehow extract the validator script to disk
  (impossible — it lives in `/$bunfs/`, only accessible to the parent
  process). Pausing the test here.
- **Filed as:** [AST-88](https://linear.app/aster69/issue/AST-88) (priority
  High, ship-blocker). Recommendation: drop branch 3 of validator_runner,
  make `process.execPath validate-ticket` recursion the unconditional
  production path. Same fix shape as AST-85.
- **Resolved 2026-05-09:** AST-88 fix shipped in v0.1.2. Confirmed
  `enqueue --linear-issue` reaches the validator successfully (next
  blocker is entry #6 below).

## 6. ✅ `gh` CLI auth for the agent user is not provisioned by the installer [ITRY-1345]

- **Symptom:** With v0.1.2 deployed (AST-88, AST-89, ITRY-1344 in place),
  `quay enqueue --linear-issue ITRY-1327` now reaches Linear, fetches the
  ticket, validates it… and then fails on a pre-flight PR-existence check:
  ```
  {"error":"internal_error",
   "message":"gh pr list --head quay/ITRY-1327 failed for test-factory-code (exit 4):
              To get started with GitHub CLI, please run:  gh auth login
              Alternatively, populate the GH_TOKEN environment variable
              with a GitHub API authentication token."}
  ```
- **Root cause:** Quay's enqueue path runs `gh pr list --head quay/<task_id>`
  for dupe detection (so an `enqueue` for an already-in-flight task lands
  on the existing branch instead of creating a new one). `gh` CLI needs
  authentication; the hermes user has none. `setup-hermes.sh` and
  `stage-secrets.sh` neither prompt for it nor provision it from the
  GitHub App PEM that's already on disk for hermes-state pushes.
- **Why latent until now:** Until v0.1.2's bundle, `enqueue` couldn't
  reach the gh-pre-flight (AST-85 / AST-88 blocked it earlier). This is
  the next-deepest layer that's now exposed.
- **Blast radius beyond enqueue:**
  1. **Enqueue dupe-check** — fails immediately, blocks task creation.
  2. **Worker** (when it spawns) — `claude --permission-mode bypassPermissions`
     will need to push commits AND open a PR on test-factory-code. The
     push works (deploy key + url.insteadOf rewrite from the install).
     The PR-open step needs HTTPS API auth, which is the same gap. So
     even if enqueue could be patched around, the worker would dead-end
     at the PR-open step.
  3. **review-pr** — also runs `gh pr` commands; same gap.
- **Locus of fix:** **hermes-agent.** Two viable shapes:
  1. **Mint a GitHub App installation token at install time, log gh in
     with it.** The fork already has the App's PEM at
     `~hermes/.hermes/auth/github-app.pem` (used for hermes-state
     pushes). Add a step to `setup-hermes.sh` that mints a token from
     it and runs `sudo -u hermes -H gh auth login --with-token`. App
     tokens expire in 1 hour — would need a refresh timer or move to
     `gh-token-helper`-style credential refresh. Most "right" but most
     plumbing.
  2. **Stage a long-lived `GH_TOKEN` in `auth/quay.env` (and `hermes.env`).**
     Operator provisions a fine-scoped PAT with read-content + write-PR
     perms on test-factory-code (and any other quay-managed repo); the
     installer threads it into the systemd unit's `EnvironmentFile`.
     Simpler, but adds another secret to the rotation surface.
  3. **Document a manual `gh auth login --with-token` step** as a
     post-install operator task in `ops/README.md`, and have
     `--verify` flag missing gh auth as drift. Cheapest, doesn't auto-
     provision but stops the silent gap.
  Recommendation: option 2 short-term (unblocks the test today), option
  1 long-term (proper auth lifecycle).
- **Workaround for this session:** Need operator to either provision a
  PAT or run `gh auth login` manually as the hermes user. Pausing until
  guidance.
- **Filed as:** [ITRY-1345](https://linear.app/inverter/issue/ITRY-1345)
  (priority High). Recommended split: short-term `GH_TOKEN` PAT in
  `auth/quay.env` to unblock today; long-term GitHub-App-installation-
  token minting wired into `setup-hermes.sh` for proper auth lifecycle.
- **Resolved 2026-05-09:** PR #40 took the long-term path directly —
  no PAT. New `ops/quay-tick-runner` mints a fresh installation token
  via `installer/hermes_github_token.py` and exports `$GH_TOKEN` before
  `exec`ing `quay tick`; `quay-tick.service` now points at the runner.
  The `quay-as-hermes` operator wrapper does the same mint inline, plus
  parses `auth/quay.env` for adapter tokens (LINEAR_API_KEY etc.), so
  ad-hoc operator commands match the tick's auth surface. Both wrap
  the helper in `timeout 30` to cap a wedged TLS handshake at the
  bash layer. Track 2 (operator-side) — `test-factory-code` added to
  the `didier-runtime` App's repo scope on github.com — confirmed live
  by `gh api repos/InverterNetwork/test-factory-code` succeeding as
  hermes after install.

---

## 7. 🟡 `bun` not on host PATH — bootstrap install_cmd fails [ITRY-1346]

- **Symptom:** With auth resolved (#6), enqueue now reaches the bootstrap
  step and fails:
  ```
  {"error":"bootstrap_failed",
   "message":"install_cmd failed (exit 127): /bin/sh: 1: bun: not found",
   "step":"install","exit_code":127,"stderr":"/bin/sh: 1: bun: not found\n"}
  ```
- **Root cause:** `deploy.values.yaml` declares for `test-factory-code`:
  ```
  quay:
    package_manager: bun
    install_cmd: "bun install"
  ```
  Quay runs `install_cmd` via `/bin/sh -c` in the worktree as `hermes`,
  with `/bin/sh`'s minimal PATH (no shell profile sourced). `bun` is
  not installed anywhere on the host:
  - `/usr/local/bin/bun` — absent
  - `/usr/bin/bun`       — absent
  - `/home/hermes/.bun/bin/bun` — absent
  - hermes' login PATH:
    `/home/hermes/.local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/snap/bin`
- **Why latent until now:** Bootstrap was never reached before — earlier
  blockers (#1, #2, #4, #5, #6) all fired upstream of `install_cmd`.
- **Locus of fix:** **hermes-agent.** Quay correctly trusts
  `install_cmd` as declared by the deployment; provisioning the
  declared package manager is a host-config concern, not a quay
  concern. Three viable shapes:
  1. **Install `bun` system-wide in `setup-hermes.sh`.** When
     `repos[].quay.package_manager == "bun"` is present in
     `deploy.values.yaml`, install the `bun` binary at
     `/usr/local/bin/bun` (download verified release tarball, idempotent
     pin in values like `quay.version`). Visible to `/bin/sh` without
     PATH gymnastics.
  2. **Per-user install + PATH wiring.** Install bun under
     `/home/hermes/.bun/bin/` and amend the systemd unit / wrapper to
     pass `PATH=/home/hermes/.bun/bin:$PATH`. More moving parts; less
     transparent for ad-hoc invocations.
  3. **Document as a host pre-req.** Operator installs bun manually
     before `setup-hermes.sh` runs; installer adds a pre-flight check
     that fails loud if any declared `package_manager` isn't on PATH.
     Cheapest, but pushes the work to operators on every fork.
  Recommendation: option 1 — matches the `quay.version` pin pattern
  (declarative, version-locked, security-boundary via SHA256). Future
  forks declaring `package_manager: pnpm` etc. follow the same shape.
- **Workaround for this session:** Installed `bun` 1.3.9 prebuilt
  linux-x64 at `/usr/local/bin/bun` ad-hoc as root, visible to
  `/bin/sh` for `hermes`. Test continues against this workaround.
- **Filed as:** [ITRY-1346](https://linear.app/inverter/issue/ITRY-1346)
  (priority Medium). Recommendation in body: declarative
  `quay.runtime_managers.bun` pin in `deploy.values.yaml`, mirrored by
  `setup-hermes.sh` with SHA verification (same security boundary as
  `quay.version`).

---

## 8. ✅ Worker exits silently — `claude` needs `-p` for non-interactive mode [PR #41]

- **Symptom:** First three quay-spawned attempts on the v0.1.2 +
  `bun`-on-host build all transitioned `queued` → `running` → `queued`
  with `event_type: crashed`, `.quay-session.log` always 0 bytes, no
  remote progress, no blocker file. Pattern:
  ```
  spawned 18:04:51 → crashed 18:05:51   (60s, 0B log)
  spawned 18:06:51 → crashed 18:07:55   (64s, 0B log)
  spawned 18:08:55 → crashed 18:09:57   (62s, 0B log)
  ```
- **Root cause:** `quay.agent_invocation` was
  `claude --permission-mode bypassPermissions < {prompt_file}` —
  missing `-p` / `--print`. Without it, claude in stdin-piped mode
  reads the prompt and exits immediately without acting; the tmux
  pane dies, quay's classifier sees no progress and no blocker, and
  schedules a crash retry.
- **Verified empirically:**
  ```
  echo "Reply with PONG" | claude -p   # → "PONG" (~3s)
  echo "Reply with PONG" | claude       # → no output, immediate exit
  ```
- **Locus of fix:** **hermes-agent** — one-line change in
  `deploy.values.yaml` to `claude -p --permission-mode bypassPermissions
  < {prompt_file}`.
- **Configs-as-code rail validation:** Tested by deleting the seeded
  `~hermes/.hermes/quay/config.toml` on krustentier and re-running
  `setup-hermes.sh`. Installer detected absence and re-rendered from
  values.yaml, producing a config.toml with the correct `-p`
  invocation and an unchanged `[adapters.linear]` block. Single source
  of truth holds; deleting the artifact = re-render-from-values is the
  canonical operator escape hatch when seeded files drift.
- **Resolved 2026-05-09:** PR #41 merged to main. Pulled on
  krustentier, wiped + re-rendered config.toml, re-ran installer.

## 9. 🟡 Worker push fails: deploy key is read-only; `GH_TOKEN` not in pane env [ITRY-1347]

- **Symptom:** With v0.1.2 + `bun` + `-p` fix in place, attempt #4
  (the first attempt on the corrected build) ran for ~64s, made the
  ITRY-1327 code change locally (commit `d9aad5f`,
  `console.log("Ran ans werk!");`), but failed at the push/PR step
  and wrote `.quay-blocked.md`:
  > *"The SSH key is read-only — `git push` fails with 'The key you are
  > authenticating with has been marked as read only'. `gh auth status`
  > reports 'You are not logged into any GitHub hosts.'. No `GH_TOKEN` /
  > `GITHUB_TOKEN` is set in the environment."*
- **Two distinct gaps surfaced:**
  1. **Push transport:** Hermes' `~/.gitconfig` rewrites
     `https://github.com/InverterNetwork/test-factory-code` to
     `git@github.com:InverterNetwork/test-factory-code` via
     `url.insteadOf`. Push goes over SSH and uses the per-repo deploy
     key, which is **read-only**. The `didier-runtime` GitHub App
     *already* has `Read & write` on this repo via its installation
     scope (verified in the App settings UI on github.com), but
     nothing in the worker codepath consumes that — the App-token rail
     and the deploy-key rail are bifurcated.
  2. **`GH_TOKEN` propagation gap:** PR #40 (ITRY-1345 fix) mints
     `GH_TOKEN` in `quay-tick-runner` before `exec`ing `quay tick`;
     verified working via `set -x` trace
     (`token=ghs_9N0d…` exported correctly). The token reaches the
     `quay tick` process and the tmux server (verified by spawning a
     test pane that printed `GH_TOKEN=<set>` from `env`). But claude
     in the *worker* pane reports `no GH_TOKEN set` — propagation is
     dropping the var somewhere between the runner-mint and the
     respawn-pane step. Possibly the tmux server is reusing a stale
     env from an earlier non-mint context, or the pane respawn
     scrubs env. Needs more digging.
- **Locus of fix:** **hermes-agent**, both gaps. Filed as
  [ITRY-1347](https://linear.app/inverter/issue/ITRY-1347) (High):
  consolidate `repos[].quay`-managed entries onto the App-token rail
  end-to-end — drop the deploy-key + `url.insteadOf` SSH rewrite,
  wire HTTPS push to consume the minted `GH_TOKEN` (credential
  helper or `gh auth setup-git`). Resolves both gaps simultaneously
  by collapsing the two auth surfaces into one.
- **Workaround:** None on the operator side without restructuring
  the worker auth path. Pausing the test here pending ITRY-1347.
- **Resolved 2026-05-09:** ITRY-1347 (PR #42) merged + ITRY-1348
  followup filed/worked-around. After installer re-run + manual
  unset of stale `url.insteadOf` in `~hermes/.gitconfig`, the
  HTTPS+App credential helper path is end-to-end validated:
  - `git push` over HTTPS via the credential helper succeeds —
    PR #1 on test-factory-code was opened by `app/didier-runtime`
    (verified via `gh pr view 1 --json author`).
  - `gh pr list --head quay/ITRY-1327` with a minted `GH_TOKEN`
    returns the PR, so quay's `prExistsForBranch` path works.

---

## 10. 🟡 Worker exits silently inside quay's tmux spawn (0-byte session log) [AST-100]

- **Symptom:** Every claude worker spawned by `quay tick` for ITRY-1327
  on krustentier exits within ~60s producing **zero bytes** in
  `.quay-session.log`, no `.quay-blocked.md`, no commit, no push. Same
  pattern as entry #8 was *before* the `-p` fix — but agent_invocation
  now has `-p` (verified live in `~hermes/.hermes/quay/config.toml`).
  Pattern from the v0.1.2 + ITRY-1347 build:
  ```
  spawned 20:29:31 → crashed 20:30:33   (62s, 0B log)
  spawned 20:31:34 → crashed 20:32:35   (61s, 0B log)
  spawned 20:33:35 → crashed 20:34:38   (63s, 0B log)
  spawned 20:35:41 → no_progress 20:36:41 (PR existed by then, from manual probe)
  spawned 20:37:43 → no_progress (budget exhausted)
  ```
- **Manual reproduction works.** Replicating quay's exact spawn
  choreography by hand (`tmux new-session -d -s … cat; pipe-pane -o;
  respawn-pane -k -c <worktree> -t <session>:0.0 "exec sh -c
  '<agent_invocation>'"`) against the same worktree, with the same
  `claude -p --permission-mode bypassPermissions < .quay-prompt.md`
  invocation, succeeds end-to-end:
  - claude makes the code edit, commits (`137a753 ITRY-1327: Update
    hello.ts copy to "Ran ans werk!"`)
  - pushes the branch via HTTPS+App credential helper
  - opens PR #1 on test-factory-code (created by `app/didier-runtime`)
  - exits cleanly, ~55s total
  - session log gets ~380 bytes (claude's final `-p` summary)
- **What's verified the same** between quay-spawn and manual-spawn:
  same agent_invocation string, same tmux choreography (each step
  shellQuote-equivalent), same worktree, same `.quay-prompt.md` (1965
  bytes), same OS user, same `~/.claude/.credentials.json`, same
  `claude --version` (2.1.132). End-to-end auth model verified
  working in the manual case.
- **Hypotheses to investigate (AST-100):**
  1. Tmux server env propagation race — claude's blocker artifact
     from earlier reported "no `GH_TOKEN` set", but `env`-dump probes
     into a fresh tmux pane show `GH_TOKEN` propagating fine.
     Possibly a stale tmux server reused across ticks with stale env.
  2. Claude credential-refresh race on the shared
     `~/.claude/.credentials.json` between concurrent processes.
  3. Pipe-pane / respawn-pane subtle interaction (signal delivery,
     pane lifetime).
  4. Systemd cgroup reaping of tmux descendants when `quay-tick.service`
     (`Type=oneshot`) exits.
- **Locus of fix:** **quay** (worker spawn substrate). Filed as
  [AST-100](https://linear.app/aster69/issue/AST-100) (priority High,
  Bug). Originally opened as ITRY-1349 in iTRY's tracker by mistake;
  re-filed in Aster (the canonical tracker for quay-side bugs) and
  the iTRY ticket canceled with a redirect comment. Recommended next
  step in the AST-100 body: wire `claude --debug --debug-file <path>`
  into `agent_invocation` temporarily and diff debug logs between
  quay-spawn and manual-spawn for the divergence point. Cross-linked
  to [AST-93](https://linear.app/aster69/issue/AST-93)'s observability
  children (AST-95 `exit_code`/`exit_signal`, AST-97 `tool_trace`)
  whose presence would make this triage nearly free.
- **Workaround for v0.1.x:** None on the operator side. The end-to-end
  auth + worker logic is functionally validated via the manual probe;
  for a deployment to drive itself without operator intervention,
  AST-100 needs to land.

---

## End-of-test summary (2026-05-09 20:42 UTC)

The v0.1.0 worker-pipeline test against ITRY-1327 on krustentier is
**complete with caveats**. End-to-end coverage achieved:

| Pipeline stage | Status | Validated by |
|---|---|---|
| `quay enqueue --linear-issue` (Linear adapter, brief synth) | ✅ | task `a1c4e61f` enqueued, brief artifact captured |
| Worktree creation + `bun install` bootstrap | ✅ | node_modules present in worktree |
| Tmux session spawn | ✅ | Pane created with `pipe-pane` log sink |
| Worker (claude) executes the brief | ✅ via manual probe; ❌ via quay's spawn | PR #1 opened in manual repro; quay-spawn → AST-100 |
| `git push` over HTTPS+App credential helper | ✅ | PR #1's commit `137a753` pushed |
| `gh pr create` via App-installation token | ✅ | PR #1 opened by `app/didier-runtime` |
| Quay classifier sees PR exists, transitions correctly | ✅ | `action: "no_progress"` (correct: PR exists, this attempt didn't push) |
| `quay review-pr <pr-url>` | ⏸ | Spec marked "Draft, not locked" — not yet implemented in v0.1.x |

**Open follow-ups filed:**
- [ITRY-1346](https://linear.app/inverter/issue/ITRY-1346) (Medium) — declarative `bun` provisioning in `setup-hermes.sh`.
- [ITRY-1348](https://linear.app/inverter/issue/ITRY-1348) (High) — installer's stale `url.insteadOf` unset has a `.git`-suffix mismatch.
- [AST-100](https://linear.app/aster69/issue/AST-100) (High) — claude worker exits silently inside quay's tmux spawn (the only thing between us and a fully autonomous green run). Originally filed as ITRY-1349 in iTRY by mistake; canceled and refiled in Aster.

**What's already shipped during the test:**
- v0.1.1 (AST-85 / AST-86 / AST-89, Linear+Slack adapter spawn fixes).
- v0.1.2 (AST-88 / ITRY-1344 / ITRY-1334 reconciler).
- PR #38 (operator wrapper cwd / silent-fallback fix).
- PR #40 (App-token mint preamble for tick + wrapper).
- PR #41 (`-p` flag for non-interactive claude — entry #8).
- PR #42 / ITRY-1347 (consolidate quay-managed repos onto App-token rail — entry #9).

**Test fixture artifact:**
- PR #1 on `InverterNetwork/test-factory-code` left open as the
  end-to-end demonstration of the worker prompt + auth path.
  Operator may merge or close at discretion; not load-bearing for
  any quay state going forward.

**Quay task `a1c4e61f-1ec9-420d-8976-b5a891e863f2`** is in the
`cancelled` state on krustentier; no live retries pending.
