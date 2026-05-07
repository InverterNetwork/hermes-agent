# quay v0.1.0 install log — krustentier (2026-05-07)

Living log of every problem hit during the first end-to-end install of `quay v0.1.0` on krustentier and its lead-up. Each entry has a **Locus of fix** field — `hermes-agent`, `quay`, `host-local`, or `TBD`. After the install completes we triage the open items and turn each into the right shape of follow-up (PR on either side, runbook entry, or accepted-as-host-only).

Status legend: ✅ resolved before/during this install, 🟡 open, 🔵 informational.

---

## 1. ✅ Quay repo was private — CI 404'd on release fetch

- **Symptom:** First post-tag CI run failed with `curl: (22) The requested URL returned error: 404` on `releases/download/v0.1.0/quay-linux-amd64`. Local unauth'd `curl -sI` against the asset also returned `404`.
- **Root cause:** `lafawnduh1966/quay` was private. GitHub returns `404` (not `401`) on unauthenticated requests for private-repo release assets, so the failure looks like "missing" rather than "forbidden."
- **Workaround:** Operator flipped the repo to public.
- **Locus of fix:** **TBD.** Three options:
  - Keep quay public — simplest, lowest friction.
  - Hermes-agent threads a `gh release download` (or `curl -H Authorization`) into `setup-hermes.sh` and grows a one-shot operator-staged token. Adds a `stage-quay-fetch-auth.sh`-shaped script.
  - Quay distributes binaries via a different surface entirely.
- **Why:** Whichever the user prefers; tonight is unblocked because the repo is public.

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
- **Locus of fix:** hermes-agent (consumer-side adapter). Could equivalently be quay-side: rename `repo_id` → `id` in the `repo list` output. Not necessarily wrong as-is — `repo_id` is more self-describing inside heterogeneous JSON output. Calling it out for the locus-of-fix triage.
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
- **Locus of fix:** hermes-agent. Could equivalently be quay-side: have the embed script emit `v${pkg.version}` so the version string matches the tag. Worth raising on quay if that's the preferred direction.
- **Why:** Convention mismatch between two reasonable choices; consumer-side strip is cheap.

## 6. 🟡 Bare-clone needs hermes-user git auth to private repos *(OPEN)*

- **Symptom:** `setup-hermes.sh` hung on `Username for 'https://github.com':` during the bare-clone step for `InverterNetwork/test-factory-code`.
- **Root cause:** `quay.repos[].url` is HTTPS; the `hermes` user (which `setup-hermes.sh` invokes via `sudo -u hermes git clone --bare`) has no GitHub credentials. The v0 limitation in `deploy.values.yaml` documents this: *"each url must be reachable by the agent user without further wiring."*
- **Tonight's workaround:** Manual host-local setup —
  1. `ssh-keygen` an `id_ed25519` keypair for the `hermes` user (no passphrase).
  2. Add the public half as a deploy key on `InverterNetwork/test-factory-code` (read-only).
  3. Pre-seed `~hermes/.ssh/known_hosts` with `ssh-keyscan github.com`.
  4. `git config --global url."git@github.com:InverterNetwork/".insteadOf "https://github.com/InverterNetwork/"` for the `hermes` user — keeps `deploy.values.yaml` HTTPS-shaped (and CI-compatible) while routing all `InverterNetwork/*` clones through SSH on the box.
- **Locus of fix:** **TBD, leaning hermes-agent.** A `stage-quay-repo-auth.sh` sibling of `stage-quay-env.sh` would formalise steps 1–4 into a single operator-run script. Same shape as existing stage-scripts: idempotent, prompts only when needed, host-local output. Avoids re-deriving the dance per repo / per host.
- **Why:** Auth shouldn't live in the binary (quay-side). The v0 design pushes it onto the operator, which is fine for one repo but doesn't scale. Formalising as a stage-script keeps the locus on hermes-agent without leaking auth into quay or into `deploy.values.yaml`.

## 7. 🔵 `url.insteadOf` paste mangled by terminal line-wrap

- **Symptom:** First attempt to set the rewrite silently no-op'd. SSH worked but bare-clone still hit HTTPS. Diagnosis (via direct SSH from the operator's Mac) showed `git config --get-regexp '^url\.'` empty and `~hermes/.gitconfig` lacked the `[url "git@github.com:InverterNetwork/"]` block.
- **Root cause:** Long single-line `git config` command line-wrapped during paste in the SSH session, with `ssh-keygen` comment text leaking into the next command.
- **Workaround:** Re-issued the command verbatim from a clean prompt.
- **Locus of fix:** Subsumed by entry #6 — a `stage-quay-repo-auth.sh` script eliminates this class of paste-error entirely (one script invocation, no manual config strings).
- **Why:** Operator UX. Solving the broader issue (a script) covers this.

---

## Triage rubric (apply once install is green)

For each open entry:

1. **Is the fix in code at all, or just operator runbook?** If runbook, document in `ops/README.md` and close.
2. **If code: hermes-agent or quay?** Default to whichever side owns the concern (auth → hermes-agent operator-staging; binary surface → quay; integration adapter → hermes-agent).
3. **Is the consumer-side adapter genuinely cheaper than the upstream change, or just easier from inside hermes-agent?** Per [feedback memory](../.claude/projects/-Users-fabianscherer-repos-inverter-brix-agents-hermes-agent/memory/feedback_hermes_quay_fix_locus.md), default to surfacing both options before patching.

Each open entry that resolves to "fix on hermes-agent" or "fix on quay" → its own PR with link back to this log.
