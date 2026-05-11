# Feedback-intake skill — draft for hermes-state

This is the canonical body of `hermes-state/skills/feedback-intake/SKILL.md`,
authored alongside the channel-trigger router (BRIX-1366) and reviewed in
this PR. On merge the operator copies the YAML+body block below into
`hermes-state/skills/feedback-intake/SKILL.md` so `hermes-sync` propagates
it to the agent's `~/.hermes/skills/` mirror; the gateway then picks it up
via `get_all_skills_dirs()` on the next reload.

The router invokes this skill whenever a top-level message lands in the
channel bound by `slack_triggers[].skill: feedback-intake` (in
`deploy.values.yaml`). The trigger envelope is prepended to the agent's
input — see `gateway/slack_triggers.py::build_envelope` for the exact
shape.

---

```markdown
---
name: feedback-intake
description: |
  Entry skill for the feedback Slack channel. Reads the trigger envelope
  prepended by the channel-trigger router, decides whether the message
  is actionable, infers the target repo, and chains to inverter-linear
  to file a BRIX ticket. Always posts a threaded reply on the source
  message — never a fresh top-level (which would re-trigger the router).
---

# Feedback-intake

You are running because the channel-trigger router fired on a top-level
message in the feedback Slack channel. The trigger envelope is at the top
of your input — read it before doing anything else.

## Envelope contract

The first block of your input is a `[Slack channel trigger]` envelope:

```
[Slack channel trigger]
channel_id: C0XXXXXXXXX
channel_name: feedback
message_ts: 1715420800.123456
permalink: https://inverternetwork.slack.com/archives/C0X.../p17154...
author.name: <real name OR synthetic_author.name on bot triggers>
author.slack_id: <U… OR synthetic_author.slack_id on bot triggers>
default_repo: iTRY-monorepo

Message:
<verbatim message body>
```

Everything below the `Message:` line is the user's verbatim text. Treat
the envelope as ground truth; never invent fields, never rewrite values.

## Behavior

1. **Read the message.** Identify what the user is reporting: bug,
   feature request, vague feedback, non-actionable noise (greeting,
   thanks, emoji-only).

2. **Decide whether to act.** If clearly non-actionable, skip filing
   and go to step 6 with a one-line reason. Otherwise proceed.

3. **Infer the target repo.** Run `quay repo list` to see the registered
   repos. Pick the one the message points at (codebase mentioned by
   name, error from a specific service, etc.). If nothing in the
   message disambiguates, fall back to `default_repo` from the envelope.

4. **File the ticket via `inverter-linear`.** Invoke the
   `inverter-linear` skill with:
   - the repo id from step 3,
   - a concise title derived from the message,
   - a description that quotes the verbatim message body and includes
     the envelope's `permalink` so the ticket links back to Slack,
   - the `author.name` from the envelope as the ticket's authors[]
     entry (this is what makes `quay validate-ticket` pass; the
     synthetic_author from bot triggers also flows through here).

   The `inverter-linear` skill handles tag-vocab fetch, ticket
   validation, `issueCreate`, and the post-create Slack permalink
   attachment — do not bypass it.

5. **Capture the BRIX-NNNN identifier** returned by `inverter-linear`.

6. **Post a threaded reply** on the trigger message
   (`message_ts` from the envelope) using `chat_postMessage` with
   `thread_ts=<envelope message_ts>`. NEVER post a fresh top-level
   message in the channel — a top-level post would re-trigger the
   router and loop. The threaded reply is the load-bearing loop
   guarantee.

   Body for a filed ticket:

       Filed [BRIX-NNNN: <title>](<linear-url>) against <repo>.

   Body for a non-actionable skip:

       Skipped — <one-line reason>. (Reply if you'd like me to file it anyway.)

## Hard rules

- Reply ONLY in the thread of the source message. Never top-level.
- One ticket per trigger event. The router dedups on `message.ts`, but
  if the user re-pings the same content the second invocation will see
  it as a new event — let `inverter-linear`'s ticket-search step
  prevent the duplicate.
- Never invoke `gh issue`. Linear is the issue tracker (see
  `inverter-linear`).
- Don't ask for confirmation before filing — the user posting in the
  feedback channel IS the consent.
- If `inverter-linear` fails (validate-ticket rejection, network
  error), post the failure reason in the threaded reply so the user
  can retry.
```
