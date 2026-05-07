# quay v0.1.0 install log — krustentier (2026-05-07)

Living log of every problem hit during the first end-to-end install of `quay v0.1.0` on krustentier and its lead-up. Each entry has a **Locus of fix** field — `hermes-agent`, `quay`, `host-local`, or `TBD`. After the install completes we triage the open items and turn each into the right shape of follow-up (PR on either side, runbook entry, or accepted-as-host-only).

Status legend: ✅ resolved before/during this install, 🟡 open, 🔵 informational.

---

## 1. ✅ Quay repo was private — CI 404'd on release fetch

- **Symptom:** First post-tag CI run failed with `curl: (22) The requested URL returned error: 404` on `releases/download/v0.1.0/quay-linux-amd64`. Local unauth'd `curl -sI` against the asset also returned `404`.
- **Root cause:** `lafawnduh1966/quay` was private. GitHub returns `404` (not `401`) on unauthenticated requests for private-repo release assets, so the failure looks like "missing" rather than "forbidden."
- **Resolution:** **Quay stays public** (decided 2026-05-07). Simplest, lowest friction; eliminates any need to thread fetch auth through the installer.
- **Locus of fix:** Resolved on the quay-side (visibility change). No code follow-up.

## 2. ✅ Bare-clone of private `InverterNetwork/test-factory-code` failed in CI

- **Symptom:** `installer-smoke` CI run failed with `fatal: could not read Username for 'https://github.com': No such device or address` when the install reached the `git clone --bare` step.
- **Root cause:** GitHub Actions runner has no credentials for `InverterNetwork/*`, and the bare-clone runs as the `hermes` system user with no auth.
- **Workaround:** PR #22 stages a local non-bare repo at `/srv/hermes/repos/quay-fixtures/<id>` in CI and `sed`s the URL in `deploy.values.yaml` to a `file://` reference. Mirrors the existing `hermes-state` fake-repo fixture.
- **Locus of fix:** hermes-agent (CI fixture only). The *runtime* version of this same problem is entry #6 below — that one is still open.
- **Why:** CI smoke should not depend on org private repos.

## 3. ✅ `quay repo list` JSON is keyed `repo_id`, not `id`

- **Symptom:** Second `setup-hermes.sh` invocation in CI produced `{"error":"duplicate_repo","message":"repo \"test-factory-code\" already exists","repo_id":"test-factory-code"}` and aborted; install was non-idempotent.
- **Root cause:** Install-time and verify-time JSON parsers in `setup-hermes.sh` read `r["id"]` and silently emitted nothing. The "already-registered" snapshot was always empty, so re-runs always re-attempted `quay repo add`.
- **Workaround:** PR #23 fixed both call sites and extracted the parser into `installer/values_helper.py` as `parse-repo-list-ids` so the field name lives in one place. New unit-test guard rejects an `id`-keyed payload.
- **Locus of fix:** hermes-agent (consumer-side adapter). Quay-side rename considered for surface consistency — tracked as **AST-82** (low priority, not blocking).
- **Why:** Integration shape mismatch caught only when the real binary first ran in CI.

## 4. ✅ `~/.hermes/quay/` ends up mode 2755 (not 755)

- **Symptom:** `--verify` reported `[DRIFT] quay data dir: mode=2755 owner=hermes:hermes (expected 755 hermes:hermes)`.
- **Root cause:** `$HERMES_HOME` is mode `02775` (setgid by design — gives the rails owner group and agent owner shared visibility). Subdirs created via `install -d -m 755` inherit the setgid bit on Linux despite the explicit `-m`.
- **Workaround:** PR #23 dropped the strict mode-equality check, kept ownership-only — matches the existing `sessions/`, `logs/`, `cache/` agent-dir pattern at `setup-hermes.sh:304`.
- **Locus of fix:** hermes-agent.
- **Why:** Verify check was over-strict for the documented permission model.

## 5. ✅ `quay --version` emits semver without the `v` prefix

- **Symptom:** Caught pre-install: `quay --version` → `0.1.0+abc1234`. `quay.version` in `deploy.values.yaml` is the tag name (`v0.1.0`). PR #19's substring compare would have fired false drift on every clean install.
- **Root cause:** `package.json` carries `0.1.0` (no `v`); `scripts/embed.ts` emits `${pkg.version}+${shortSHA}`. Tag-vs-semver convention difference.
- **Workaround:** PR #21 strips leading `v` from the pin (`${quay_version#v}`) before the substring-compare. Fetch URL stays unaffected because the tag name is required there.
- **Locus of fix:** hermes-agent (shipped). Quay-side alignment (embed `v${pkg.version}`) tracked as **AST-81** (low priority, not blocking).
- **Why:** Convention mismatch between two reasonable choices; consumer-side strip is cheap.

## 6. 🟡 Bare-clone needs hermes-user git auth to private repos

- **Symptom:** `setup-hermes.sh` hung on `Username for 'https://github.com':` during the bare-clone step for `InverterNetwork/test-factory-code`.
- **Root cause:** `quay.repos[].url` is HTTPS; the `hermes` user (which `setup-hermes.sh` invokes via `sudo -u hermes git clone --bare`) has no GitHub credentials. The v0 limitation in `deploy.values.yaml` documents this: *"each url must be reachable by the agent user without further wiring."*
- **Tonight's workaround (live on krustentier):** Manual host-local setup —
  1. `ssh-keygen` an `id_ed25519` keypair for the `hermes` user (no passphrase).
  2. Add the public half as a deploy key on `InverterNetwork/test-factory-code` (read-only).
  3. Pre-seed `~hermes/.ssh/known_hosts` with `ssh-keyscan github.com`.
  4. `git config --global url."git@github.com:InverterNetwork/".insteadOf "https://github.com/InverterNetwork/"` for the `hermes` user — keeps `deploy.values.yaml` HTTPS-shaped (and CI-compatible) while routing all `InverterNetwork/*` clones through SSH on the box.
- **Locus of fix:** **hermes-agent.** A `stage-quay-repo-auth.sh` sibling of `stage-quay-env.sh` formalises steps 1–4 into a single operator-run script. Same shape as existing stage-scripts: idempotent, prompts only when needed, host-local output. Avoids re-deriving the dance per repo / per host. The manual setup proved the design works end-to-end (install completed at HEAD `6a0a883a`); now it just needs codifying. **Tracked in ITRY-1311** (bundled with #9 and #10).
- **Why:** Auth shouldn't live in the binary (quay-side). The v0 design pushes it onto the operator, which is fine for one repo but doesn't scale. Formalising as a stage-script keeps the locus on hermes-agent without leaking auth into quay or into `deploy.values.yaml`.

## 8. ✅ `git remote get-url` applies `insteadOf` rewrite, breaks idempotency check — fixed in PR #25

- **Symptom:** With the SSH key + `insteadOf` rewrite from entry #6 in place, re-running `setup-hermes.sh` aborted with `FAIL: /home/hermes/.hermes/quay/repos/test-factory-code.git origin=git@github.com:InverterNetwork/test-factory-code, expected https://github.com/InverterNetwork/test-factory-code … refusing to silently re-point an existing bare clone`.
- **Root cause:** The script's bare-clone idempotency check (and the matching `--verify` check) read the existing origin via `git remote get-url origin`, which **applies `insteadOf` rewrites at output time**. Diagnosis on krustentier confirmed the raw stored URL in `.git/config` was the HTTPS form (matching `quay.repos[0].url`) — only `get-url` handed back the rewritten SSH form. The bare clone itself was functionally correct; fetches routed via SSH transparently. The check was wrong, not the data.
- **Resolved in:** PR #25 — switched both compare sites to `git config --get remote.origin.url` (raw stored value, no rewrite applied). Plus a regression test (`test_origin_check_ignores_insteadof_rewrite`) that configures an `insteadOf` in the bare clone's local config and asserts no false drift.
- **Locus of fix:** hermes-agent (consumer-side bug; the data layer was correct).
- **Why latent until now:** Every prior install (CI smoke fixture, dev-machine local) used a single-protocol path. The bug only fires when the operator legitimately bridges HTTPS↔SSH for the agent user, which only happens at real-host install time.

## 9. 🟡 Worker auth: subscription vs. metered API key undocumented

- **Symptom:** `stage-quay-env.sh` prompts for `ANTHROPIC_API_KEY` as optional, suggesting it's the canonical worker-auth path. But `quay.agent_invocation` runs `claude` (the CLI) which prefers `ANTHROPIC_API_KEY` over a `claude login` subscription session when both are present. An operator with a paid plan could silently get billed against the metered API instead of using their subscription.
- **Root cause:** Two independent auth surfaces (subscription via `claude login`, metered via env var) and no guidance in the installer on which to pick. `claude login` also needs to run as the `hermes` user so credentials live in `~hermes/.claude/` and survive `quay-tick` worker invocations — undocumented today.
- **Tonight's workaround:** Operator-side discipline. Leave `ANTHROPIC_API_KEY` blank when running `stage-quay-env.sh` (the prompt accepts empty input → omits the line from `quay.env`); run `sudo -u hermes -H claude login` separately.
- **Locus of fix:** **hermes-agent.** Two cheap improvements:
  1. `stage-quay-env.sh` should clarify the prompt: *"ANTHROPIC_API_KEY (optional — leave blank if using `claude login` subscription auth)"*. One-line text change.
  2. `ops/README.md#quay-tick` (or a new `ops/README.md#worker-auth`) should document the two auth modes and explicitly recommend running `claude login` as the agent user post-install.
  3. Optionally: add a `--check` mode to `stage-quay-env.sh` that detects `~hermes/.claude/auth.json` (or whatever the credential store is) and warns if both subscription auth and `ANTHROPIC_API_KEY` are present.

  **Tracked in ITRY-1311** (bundled with #6 and #10).
- **Why:** Same flavour of operator-experience issue as entry #6 — the v0 design pushes auth onto the operator, but doesn't surface the choice clearly. Quay-side has nothing to do here; this is purely about the hermes-agent installer's UX.

## 10. 🟡 `claude` CLI not installed on a fresh box

- **Symptom:** `sudo -u hermes -H claude login` → `sudo: claude: command not found` on krustentier despite a clean `setup-hermes.sh` install.
- **Root cause:** Apt-prep installs `python3.12-venv`, build deps, etc., but the Anthropic CLI is not in any apt repo and not part of `setup-hermes.sh`'s provisioning. The v0 design treats `claude` as an operator-installed prerequisite — but doesn't say so anywhere.
- **Tonight's workaround:** Operator runs `curl -fsSL https://claude.ai/install.sh | bash` to install the binary, then `sudo -u hermes -H claude login` to authenticate.
- **Locus of fix:** **hermes-agent.** Two options worth weighing:
  1. Add a check in `setup-hermes.sh`: when `quay.version` is set and `quay.agent_invocation` references `claude`, refuse to install unless `claude` is on PATH. Cheap, fail-loud.
  2. Document in `ops/README.md#quay-tick`: "before staging tokens, install `claude` via `curl -fsSL https://claude.ai/install.sh | bash` and run `sudo -u hermes -H claude login`." Pure runbook; no code change.
  3. (Heavier) Have `setup-hermes.sh` install the binary if missing. Adds a third-party download to apt-prep, raises the supply-chain surface — probably not worth it for v0.
- **Why:** Same family as #6 and #9 — the install path assumes operator already has the runtime tools, but doesn't signal which ones. Combining the three into a single hermes-agent PR (`stage-quay-repo-auth.sh` + ops/README "before you start" section) is the cleanest follow-up shape.

  **Tracked in ITRY-1311** (bundled with #6 and #9).

## 11. 🔵 Bare clone + registration disappeared once between install and verify (unreproducible)

- **Symptom:** First `--verify` run after the post-#25 install reported `[DRIFT] quay repo test-factory-code: bare clone missing` even though the install minutes earlier had logged `==> quay bare clone test-factory-code present (preserving)` and `==> registering test-factory-code with quay`. Direct inspection confirmed `/home/hermes/.hermes/quay/repos/` was empty AND `quay repo list` returned `[]`.
- **What we tried:** Re-ran the installer (recreated bare clone + registration), then watched the dir + `quay repo list` every 35s for 5 minutes (~5 `quay-tick` fires). State held perfectly across every snapshot.
- **Root cause:** Unknown — not reproduced. Quay's source has no automatic bare-clone cleanup path, so `quay tick` itself isn't the culprit. Possible triggers (none confirmed): a transient race during the earlier broken-symlink claude install state, the earlier mid-paste partial commands the terminal handled weirdly, or something related to the install's `==> registering` step running while a `quay tick` was in flight.
- **Locus of fix:** **None until it reproduces.** Logging it here so if anyone hits it again, they have a starting point and aren't surprised.
- **Why:** Untracked transient state mutations in a complex multi-process install path are sometimes one-time artifacts. The installer is idempotent — re-running cleanly recovered. If we see this again, the next debug step is to enable `quay tick`'s NDJSON output to journal (currently silent on success; `StandardOutput=journal` is already the default but quay isn't logging) and add a tripwire that snapshots the data dir on every tick.

## 7. 🔵 `url.insteadOf` paste mangled by terminal line-wrap

- **Symptom:** First attempt to set the rewrite silently no-op'd. SSH worked but bare-clone still hit HTTPS. Diagnosis (via direct SSH from the operator's Mac) showed `git config --get-regexp '^url\.'` empty and `~hermes/.gitconfig` lacked the `[url "git@github.com:InverterNetwork/"]` block.
- **Root cause:** Long single-line `git config` command line-wrapped during paste in the SSH session, with `ssh-keygen` comment text leaking into the next command.
- **Workaround:** Re-issued the command verbatim from a clean prompt.
- **Locus of fix:** Subsumed by entry #6 — a `stage-quay-repo-auth.sh` script eliminates this class of paste-error entirely (one script invocation, no manual config strings).
- **Why:** Operator UX. Solving the broader issue (a script) covers this.

---

## Triage outcome (2026-05-07)

Install completed green (40 checks, 0 drift). Open entries triaged into:

- **ITRY-1311** (Inverter, hermes-agent): bundled follow-up covering entries #6, #9, #10 — `stage-quay-repo-auth.sh` + worker-auth runbook + `claude` CLI prerequisite. https://linear.app/inverter/issue/ITRY-1311
- **AST-81** (Aster, quay): emit `--version` with leading `v` to match git tag (entry #5, low priority — consumer workaround already shipped). https://linear.app/aster69/issue/AST-81
- **AST-82** (Aster, quay): rename `repo_id` → `id` in `repo list` output for surface consistency (entry #3, low priority — consumer fix already shipped). https://linear.app/aster69/issue/AST-82

Entry #11 (unreproducible state loss) stays as a watch-list note — no action unless it recurs. Entries #1, #2, #4, #7, #8 fully resolved (✅ or 🔵 in the per-entry headers).

### Triage rubric (kept for future installs)

1. **Is the fix in code at all, or just operator runbook?** If runbook, document in `ops/README.md` and close.
2. **If code: hermes-agent or quay?** Default to whichever side owns the concern (auth → hermes-agent operator-staging; binary surface → quay; integration adapter → hermes-agent).
3. **Is the consumer-side adapter genuinely cheaper than the upstream change, or just easier from inside hermes-agent?** Per [feedback memory](../.claude/projects/-Users-fabianscherer-repos-inverter-brix-agents-hermes-agent/memory/feedback_hermes_quay_fix_locus.md), default to surfacing both options before patching.
