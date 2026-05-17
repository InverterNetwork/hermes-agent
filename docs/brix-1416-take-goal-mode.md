# BRIX-1416: `/take --goal` for goal-mode Quay tasks

`/take` is the only manual Hermes surface for enqueuing Quay work. Today every
`/take BRIX-XXXX` enqueues an `oneshot` task. Quay now also supports goal-mode
tasks via `worker_execution: goal` in the Linear `quay-config` block. This doc
specifies how the `/take` skill exposes that without growing a new gateway
command path.

The actual implementation lives in the `/take` skill (`skills/quay/take/SKILL.md`
in `hermes-state`). This doc is the contract that skill must satisfy.

## Syntax

```text
/take BRIX-1234            # default oneshot enqueue, unchanged
/take --goal BRIX-1234     # request goal-mode enqueue
```

`--goal` is the only new flag. It MUST appear before the ticket id. Anything
else is a usage error and is rejected with the existing one-line usage
response (extended to mention the flag).

## `worker_execution` semantics

| Ticket state | `/take BRIX-1234` | `/take --goal BRIX-1234` |
| --- | --- | --- |
| No `quay-config` block | infer block, omit `worker_execution` (defaults to oneshot in Quay), enqueue | infer block, write `worker_execution: goal`, run suitability gate, enqueue if gate passes |
| Block present, no `worker_execution` | enqueue as-is | run suitability gate; if gate passes, write `worker_execution: goal` into the existing block (append the field, preserve every other field), then enqueue |
| Block present, `worker_execution: oneshot` | enqueue as-is | **conflict** — do not silently override. Reply with the conflict message below and stop. |
| Block present, `worker_execution: goal` | treat block as authoritative, enqueue as goal | treat block as authoritative, enqueue as goal (gate skipped — see "Where the gate runs") |

Existing-block writes append the `worker_execution` field without rewriting
the rest of the block. Re-fetch and verify the field landed before enqueue,
same contract as the existing repo/tags/authors verify pass.

### Conflict message

```
BRIX-1234 already declares worker_execution: oneshot in its quay-config block.
`/take --goal` will not silently override that. To switch to goal mode, edit
the ticket's quay-config block (set worker_execution: goal) and re-run
`/take BRIX-1234`, or run `/take BRIX-1234` to honor the existing oneshot
declaration.
```

## Suitability gate

Goal-mode tasks can spend substantially more tokens than oneshot. `/take --goal`
MUST NOT enqueue immediately on the strength of a flag alone. Before enqueue
(and before any `save_issue` writeback of `worker_execution: goal`), the skill
runs an explicit suitability check on the ticket.

References for the gate criteria:

* <https://developers.openai.com/cookbook/examples/codex/using_goals_in_codex>
* <https://developers.openai.com/codex/use-cases/follow-goals>

### Gate questions

The skill must answer all of:

1. Is there one durable objective (not a one-off edit)?
2. Is the objective clear enough to run for a long time without repeated human clarification?
3. Is there a verifiable stopping condition?
4. Is there a validation surface — tests, benchmark, report, artifact, command output, or concrete review evidence?
5. Is the scope bounded enough for one Quay task (bigger than a prompt, smaller than a backlog)?
6. Does the ticket carry enough context / spec detail for a durable goal objective?
7. Is goal mode actually needed, or would normal oneshot suffice?
8. Are repo / tags / authors / base config unambiguous (i.e. the same bar the regular `/take` clean path enforces)?

The gate is lightweight. It is a suitability check, not a planning phase.

### Outcomes

The gate must classify into exactly one of three outcomes:

1. **Suitable for goal mode.** Durable objective, bounded scope, enough context, verifiable stopping condition. Proceed: write/preserve `worker_execution: goal`, verify, enqueue.
2. **Needs small ticket changes before goal mode.** Goal mode looks appropriate, but the ticket is missing minor information (validation command, stopping condition, base branch, a short acceptance criterion, …). Do NOT enqueue. Reply with concrete suggested ticket edits the operator can apply. After the operator updates the ticket, `/take --goal BRIX-XXXX` can be retried.
3. **Not suitable for goal mode.** Work is too small, too vague, too broad, mixed-scope, lacks a plausible validation surface, or would need ongoing human steering. Reject. Suggest plain `/take BRIX-XXXX`, splitting the ticket, or rewriting it into a proper goal objective.

Outcome 2 ("needs small ticket changes") must not be reported as a hard
rejection — the operator should get actionable ticket-edit guidance before any
tokens are spent on a goal task.

The chosen outcome and its reason must appear in the Slack reply so the
operator can see why goal mode was accepted, deferred for edits, or rejected.

### Where the gate runs

For a flag-driven goal request (`/take --goal BRIX-1234`):

* Run the gate AFTER inference (repo / tags / authors) and BEFORE the
  `save_issue` that adds `worker_execution: goal`. Outcomes 2 and 3 leave the
  ticket completely untouched.

For a ticket that already declares `worker_execution: goal` in its
`quay-config` block (operator-authored):

* Trust the operator: the block is authoritative. Do not re-run the gate, do
  not block enqueue.

## Confirmation / weird path

The existing `/take` "weird inference" confirmation path already renders the
inferred `quay-config` block back to the operator before any writeback. When
`--goal` was requested, the previewed block MUST include the `worker_execution:
goal` line so the operator can see what is about to land. The reasoning section
gains one bullet covering `worker_execution`.

## Acceptance criteria

* `/take BRIX-1234` continues to enqueue a default oneshot task with no behavior change.
* `/take --goal BRIX-1234` writes `worker_execution: goal` into the inferred or existing `quay-config` block before enqueue, only after the suitability gate passes.
* A ticket whose `quay-config` already contains `worker_execution: goal` is treated as authoritative by both `/take` and `/take --goal`; the gate runs only for the explicit flag path.
* `/take --goal BRIX-1234` against a ticket whose `quay-config` declares `worker_execution: oneshot` surfaces a clear conflict message and does NOT silently override.
* The weird/confirmation path previews include `worker_execution` when `--goal` was requested.
* The Slack reply names the gate outcome (suitable / needs-edits / not-suitable) and the reason.
* The `/take` SKILL.md docs/examples are updated to show `--goal` syntax and the gate.

## Out of scope

* No new gateway command path. `--goal` is parsed by the skill, not the gateway router.
* Quay still owns execution semantics. Hermes only sets the existing `worker_execution` field; it does not introduce new Quay knobs.
* Multi-flag combinations beyond `--goal` are out of scope for v1.
