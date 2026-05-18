# OpenClaw vs Hermes Capability Audit

Snapshot date: 2026-05-18  
Host inspected: `krustentier`  
Scope: deployed OpenClaw container and deployed Hermes Agent instance.

This audit lists credential surfaces and the skills or runtime paths that use them. It intentionally does not include raw secret values.

## Summary

Hermes has already replaced the core AI-automation lane:

- Slack gateway
- Linear issue creation/intake
- GitHub App based code work
- Quay task queue, workers, reviewers, review ingress, and orchestration
- Codex/Claude runtime auth for agent execution
- Local repo mirrors for supported repos
- AWS read-only diagnostics for dev/staging/test and prod (BRIX-1426: skills
  `aws-lambda-debug` and `aws-lambda-debug-prod` in hermes-state, env staged
  via `stage-secrets.sh` into `auth/ops.env` and `auth/ops-prod.env`)
- DynamoDB read-only query workflows (BRIX-1426: skills `dynamodb-query` and
  `dynamodb-query-prod`, IAM-gated, table name passed as an argument)
- New Relic Lambda diagnostics via NerdGraph (BRIX-1426: skill
  `new-relic-lambda`, `NEW_RELIC_API_KEY` in `auth/ops.env`)

OpenClaw still has several operational capabilities that are not obviously ported:

- Onchain state/order operations using keystores
- Google Drive service-account access
- GitBook access
- Gmail access
- Telegram bot/coordinator transport
- Slack digest promotion / PKB maintenance flows

## Runtime Secret Surfaces

### OpenClaw

Runtime env in `openclaw-openclaw-gateway-1` includes:

| Area | Credential names | Associated skills or flows |
|---|---|---|
| Slack | `SLACK_BOT_TOKEN`, `SLACK_APP_TOKEN`, `SLACK_SIGNING_SECRET`, `SLACK_HEALTH_BOT_TOKEN`, webhook URLs | `slack`, `digest-promote`, `linear-tickets`, Slack digest/channel logs, notification plumbing |
| Telegram | `TELEGRAM_BOT_TOKEN`, `TELEGRAM_BOT_TOKEN_COORDINATOR` | gateway/cron transport, coordinator delivery |
| GitHub | `GITHUB_PAT` | `github`, `gh-issues`, `review-pr`, `changelog-sync`, `self-update` |
| Linear | `LINEAR_API_KEY` | `linear-tickets`, `task-runner`, `review-pr` |
| AWS dev/staging/test | `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_DEFAULT_REGION` | `debug-lambda`, `query-database`, some `onchain-order` reads |
| AWS prod | `AWS_PROD_ACCESS_KEY_ID`, `AWS_PROD_SECRET_ACCESS_KEY` | `debug-lambda`, `query-database` with explicit prod prefix |
| New Relic | `NEW_RELIC_API_KEY` | `debug-lambda` |
| Google | `GOOGLE_SA_KEY_PATH` plus `/root/.openclaw/google-sa-key.json` | `google-access`, Google Docs/Sheets/Slides read access |
| Gmail | `GMAIL_USER`, `GMAIL_APP_PASSWORD` | credential present; no strong custom skill match found |
| GitBook | `GITBOOK_API_TOKEN`, `GITBOOK_BRIX_SPACE_ID` | credential present; no strong custom skill match found |
| LLM/image | `ANTHROPIC_API_KEY`, `GEMINI_API_KEY`, `NANO_BANANA_PRO_API_KEY` | `linear-tickets` helper, `summarize`, `nano-banana-pro` |
| OpenAI/Codex | OpenAI Codex OAuth files under OpenClaw agent auth profiles | coding/task-runner agent execution |
| OpenClaw API | `OPENCLAW_GATEWAY_TOKEN`, API key hashes in `/root/.openclaw/api/keys.json` | OpenClaw API/gateway auth |
| Device identity | device private key and operator token under `/root/.openclaw/identity/` | OpenClaw device/operator auth |
| Onchain | `KEYSTORE_PASSWORD`, `ORDERBOT_KEYSTORE_PASSWORD`, keystores under `/root/.openclaw/keystore/`, raw Blockrun wallet key | `onchain-order`, `onchain-state`, order bot |
| Bug tracker | `BUG_TRACKER_SHEET_ID`, `BUG_TRACKER_SHEET_TAB`, `BUG_TRACKER_SLACK_WEBHOOK_URL` | bug-tracker sync scripts |

OpenClaw also has backup env files at `/root/.openclaw/.env.bak.*` containing the same env-key set.

### Hermes

Live Hermes secret files under `/home/hermes/.hermes/auth` expose:

| Area | Credential names | Associated skills or flows |
|---|---|---|
| Slack | `SLACK_BOT_TOKEN`, `SLACK_APP_TOKEN`, allowed-users/channels runtime env | Hermes Slack gateway, `feedback-intake`, `inverter-linear`, `inverter-customer-request`, Quay orchestrator Slack replies |
| Linear | `LINEAR_API_KEY`, `LINEAR_TEAM_ITRY`, `LINEAR_TEAM_BRIX` | built-in `productivity/linear`, custom `inverter-linear`, `inverter-customer-request`, Quay `/take` |
| GitHub worker app | `HERMES_GH_APP_ID`, `HERMES_GH_INSTALLATION_ID`, `HERMES_GH_APP_KEY` | token helper, `hermes-code-sync`, Quay workers, GitHub skills via minted token |
| GitHub reviewer app | reviewer App config and token cache under `/etc/hermes` / `/run/hermes` | Quay reviewer attempts |
| Quay HTTP ingress | `QUAY_REVIEW_PR_TOKEN`, `API_SERVER_KEY` | Hermes API server `/quay/review-pr` |
| Codex | `/home/hermes/.codex/auth.json`, `/home/hermes/.hermes/auth.json` credential pool | Hermes/Quay Codex workers and reviewers |
| Claude | `/home/hermes/.claude/.credentials.json` | Hermes/Quay Claude workers and reviewers |
| Gateway runtime | `GATEWAY_ALLOW_ALL_USERS`, `SLACK_ALLOWED_USERS`, `SLACK_ALLOWED_CHANNELS` | Slack access control |
| AWS dev/staging/test | `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_DEFAULT_REGION` (staged into `auth/ops.env`) | `aws-lambda-debug`, `dynamodb-query` |
| AWS prod | `AWS_PROD_ACCESS_KEY_ID`, `AWS_PROD_SECRET_ACCESS_KEY` (staged into `auth/ops-prod.env` — separate file for visible prod surface) | `aws-lambda-debug-prod`, `dynamodb-query-prod` |
| New Relic | `NEW_RELIC_API_KEY` (staged into `auth/ops.env`) | `new-relic-lambda` |

Hermes does not currently show live env creds for GitBook, Gmail, Telegram, Google service account, or onchain keystores.

## Skill Coverage Matrix

| Capability | OpenClaw status | Hermes status | Porting read |
|---|---|---|---|
| Slack conversation gateway | Present | Present | Already covered, but channel/trigger semantics differ. |
| Slack feedback intake | Partial/custom | Present | Hermes likely owns this now. |
| Slack direct API read | Present via skill docs and `SLACK_BOT_TOKEN` | Possible via token, but skills intentionally prefer gateway tooling | No urgent port unless operators need raw Slack API helpers. |
| Linear issue search/create/update | Present: `linear-tickets` | Present: `productivity/linear`, `inverter-linear`, Quay `/take` | Covered. |
| Linear contributor extraction from Slack | Present, uses Slack + Anthropic | Present, custom `inverter-linear` helper uses Slack without Anthropic | Covered, simpler in Hermes. |
| Customer-request extraction | Not clearly central | Present: `inverter-customer-request` | Hermes is ahead here. |
| GitHub PR/issue work | Present via PAT | Present via GitHub App + Quay | Covered; Hermes architecture is cleaner. |
| Automated coding tasks | Present: `task-runner` | Present: Quay | Covered by Quay; OpenClaw task-runner can be retired after confidence. |
| PR review | Present: `review-pr` | Present: Quay reviewer + GitHub code-review skills | Covered. |
| Repo mirror/code search | Present: qmd/code-index/slack digest collections | Present: `/home/hermes/.hermes/code` mirrors and `local-codebase-lookup` | Partially covered. Semantic qmd-style search may still need explicit port if used. |
| AWS Lambda debugging | Present: `debug-lambda` | Covered (BRIX-1426): `aws-lambda-debug` (non-prod) + `aws-lambda-debug-prod` (prod), env staged into `auth/ops.env` / `auth/ops-prod.env` | Covered. Prod split is two skills so picking the wrong one is hard to do by accident. |
| DynamoDB data queries | Present: `query-database` | Covered (BRIX-1426): `dynamodb-query` (non-prod) + `dynamodb-query-prod` (prod), IAM-gated, table name passed as an argument | Covered. No skill-side allow-list — IAM is the boundary. |
| New Relic diagnostics | Present: `debug-lambda` | Covered (BRIX-1426): `new-relic-lambda`, `NEW_RELIC_API_KEY` in `auth/ops.env` | Covered. Single account scope; account ID passed as a call argument. |
| Onchain state reads | Present: `onchain-state` | Missing custom skill/creds | Port if bot should answer token/contract state questions. |
| Onchain order execution | Present: `onchain-order` + orderbot | Missing custom skill/creds | High-risk. Port only with explicit safety model and confirmation gates. |
| Google Docs/Sheets/Slides read access | Present via service account | Built-in `google-workspace` exists, but no live service-account/OAuth config found | Port credential/config if users rely on sharing Google files to the bot. |
| Gmail | Creds present | Built-in email/himalaya skill exists, no live Gmail creds found | Clarify whether this is used; likely low priority. |
| GitBook | Creds present | No live creds or clear skill found | Clarify use. Could be docs sync/search gap. |
| Telegram | Present transport tokens | Hermes gateway supports Telegram generally, but no live tokens found | Port only if Telegram users/channels still matter. |
| Slack digest promotion / PKB | Present: `digest-promote`, `knowledge-base`, `knowledge-refresh` | No direct equivalent found | Port if Didier PKB/slack-digest flow is still valuable. |
| Bug tracker sheet sync | Present scripts/env | No Hermes equivalent found | Clarify if still active. |
| Image generation via Gemini/Nano Banana | Present | Hermes has image-related skills, but no live Gemini/Nano key found | Low/medium priority depending on usage. |
| OpenClaw API keys/device identity | Present | Not applicable | Do not port; retire with OpenClaw. |

## Concrete Port Candidates

### High Priority

1. **AWS/New Relic diagnostics** — **Done (BRIX-1426).**
   - Skills: `aws-lambda-debug` and `aws-lambda-debug-prod` (split intentionally
     so prod is impossible to invoke by accident); `new-relic-lambda` for NRQL.
   - Env: `AWS_*` + `NEW_RELIC_API_KEY` in `auth/ops.env`; `AWS_PROD_*` in
     `auth/ops-prod.env` (separate file = visible at the filesystem level).
   - Read-only by construction.

2. **DynamoDB query workflow** — **Done (BRIX-1426).**
   - Skills: `dynamodb-query` and `dynamodb-query-prod`.
   - Table name is an argument; IAM gates access (no skill-side allow-list).
   - Read-only by construction.

3. **Google file access**
   - Decide whether Hermes should use service-account JSON or OAuth.
   - Wire the existing `google-workspace` skill to deployed credentials.
   - Port the simple “share with this service account” guidance.

### Medium Priority

4. **Onchain state reads**
   - Port read-only parts first.
   - Keep write/order execution separate.

5. **Slack digest / PKB**
   - Decide whether Hermes should continue maintaining Didier PKB and Slack digests.
   - If yes, port `digest-promote`, `knowledge-base`, and `knowledge-refresh` concepts.

6. **GitBook**
   - First confirm what the GitBook token currently does.
   - If it backs docs sync/search, port as a narrow docs capability.

### Low Priority / Needs Confirmation

7. **Gmail**
   - Credential exists on OpenClaw, but I did not find a strong custom skill using it.
   - Port only if there is an active email workflow.

8. **Telegram**
   - Port only if users still interact with the bot on Telegram.

9. **Image generation keys**
   - Port only if Nano Banana/Gemini image generation is still used operationally.

10. **Bug tracker sheet sync**
   - Needs owner confirmation.

## Do Not Port As-Is

- OpenClaw gateway token.
- OpenClaw API key hashes.
- OpenClaw device identity/operator token.
- OpenClaw-specific auth profile layout.
- Backup env files.

These are implementation details of the system being sunset.

## Notes And Risks

- OpenClaw stores broad operational powers in one runtime container. Hermes currently has a narrower, more purpose-built auth surface.
- The dangerous delta is onchain write/order execution. If ported, it should not simply become another always-available skill. It needs explicit confirmation, audit logging, and likely role/channel restrictions.
- AWS prod access should stay visibly distinct from non-prod. OpenClaw handled this by requiring explicit prod env-prefixing.
- Hermes’ GitHub model is better than OpenClaw’s PAT model because it uses app installation tokens and separate reviewer identity.
- Hermes already has many generic built-in skills, but without deployed credentials those are not equivalent to OpenClaw’s live capabilities.

## Recommended Sunset Checklist

- [x] Decide whether AWS/New Relic diagnostics must survive OpenClaw. **Yes — ported under BRIX-1426.**
- [x] Decide whether database query workflows must survive OpenClaw. **Yes — ported under BRIX-1426 (DynamoDB read-only).**
- [ ] Decide whether Google Drive sharing to the bot must survive OpenClaw.
- [ ] Decide whether onchain reads and order execution must survive OpenClaw.
- [ ] Decide whether Slack digest / PKB promotion must survive OpenClaw.
- [ ] Confirm whether Gmail, GitBook, Telegram, bug-tracker sheet sync are still used.
- [ ] Remove OpenClaw backup env files or archive them securely before shutdown.
- [ ] Rotate any credentials that were only needed by OpenClaw after shutdown.

