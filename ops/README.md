# Operator runbook: hermes-sync

This directory holds the periodic two-way sync that keeps the agent's local
state repo (`~/.hermes/state/`) in step with the remote on GitHub. Inline
commit hooks already record skill writes and session-start memory snapshots
synchronously; this timer covers everything else (mid-session memory writes,
cron edits) and ships local commits to the remote.

## Files

| Path                                | Purpose                                      |
| ----------------------------------- | -------------------------------------------- |
| `ops/hermes-sync`                   | The sync script. Installed to `/usr/local/sbin/hermes-sync`, root-owned. |
| `ops/hermes-sync.service`           | systemd service unit. `User=__AGENT_USER__` is templated by `setup-hermes.sh`. |
| `ops/hermes-sync.timer`             | systemd timer unit. 2-min cadence. |

`com.hermes.sync.plist` (launchd) is not shipped yet — the installer is
Linux/systemd-only as of v0.1. Tracked under the macOS TODO at the top of
`installer/setup-hermes.sh`.

## Install

`installer/setup-hermes.sh` handles everything. Re-running is idempotent:

```sh
sudo installer/setup-hermes.sh \
  --state /home/hermes/.hermes/state \
  --auth-method app
```

Re-run touches:

* `/usr/local/sbin/hermes-sync` (overwritten — it's code, not config)
* `/etc/systemd/system/hermes-sync.service` (overwritten with current
  `__AGENT_USER__` substitution)
* `/etc/systemd/system/hermes-sync.timer` (overwritten)
* `/etc/default/hermes-sync` — **preserved** if it exists. Delete to force
  regeneration from defaults.

## Cadence

2-minute interval, set in `hermes-sync.timer`:

```ini
[Timer]
OnUnitActiveSec=2min
```

To retune without editing the shipped unit, drop a systemd override:

```sh
sudo systemctl edit hermes-sync.timer
# In the editor, add:
# [Timer]
# OnUnitActiveSec=
# OnUnitActiveSec=5min
sudo systemctl daemon-reload
```

The empty assignment is required to clear the inherited value before
setting your own — systemd otherwise appends.

## Logs

Output goes to the systemd journal. Tail the live stream:

```sh
sudo journalctl -u hermes-sync.service -f
```

Sample of a healthy tick:

```
hermes-sync[NNN]: [hermes-sync] synced @ 22236f0
```

A push retry-and-continue (remote unreachable, transient auth lapse, etc.)
exits 0 so systemd doesn't mark the unit failed. Look for `WARN`:

```
hermes-sync[NNN]: [hermes-sync] WARN: git push origin main failed (will retry on next tick)
```

## Conflict resolution flow

If a remote pull rebase fails, the script:

1. Aborts the rebase via `git rebase --abort` (the `--autostash` is
   automatically restored by abort — local mid-session work is preserved).
2. Writes `~/.hermes/state/CONFLICT.md` describing the divergence.
3. Exits non-zero so the operator notices in `journalctl -u hermes-sync`.
4. **All subsequent ticks refuse to run** until `CONFLICT.md` is removed.

To resolve:

```sh
cd ~/.hermes/state
git status                          # inspect divergence
git pull --rebase origin main       # re-run; resolve conflicts as prompted
git push origin main
rm CONFLICT.md                      # unblocks the next tick
```

The next timer tick (within 2 min) picks up cleanly.

## Environment overrides

`/etc/default/hermes-sync` is sourced by `hermes-sync.service`. Default
contents:

```sh
HERMES_STATE_DIR=/home/hermes/.hermes/state
HOME=/home/hermes
```

Override `HERMES_STATE_DIR` if the install lives outside the default
layout. The script re-resolves on every invocation, so changes here take
effect on the next tick.

`HOME` is needed because git's credential helper resolves
`~/.config/git/credentials` relative to `$HOME`, and systemd doesn't set
`$HOME` by default.

## Manual operations

Force a tick:

```sh
sudo systemctl start hermes-sync.service
journalctl -u hermes-sync.service -n 30 --no-pager
```

Stop the timer (keeps the script in place):

```sh
sudo systemctl disable --now hermes-sync.timer
```

Inspect what's about to be pushed:

```sh
sudo -u <agent_user> git -C ~<agent_user>/.hermes/state log --oneline @{u}..
```

## Single-writer invariant

Three processes can write commits to the state repo:

1. The agent's inline `skill_manage` hook (one per skill_manage call).
2. The agent's session-start hook (one per brand-new session).
3. `hermes-sync` (one per timer tick).

In-process exclusion (1 ↔ 2 in the same agent) is handled by a Python
`threading.RLock` in `agent/state_repo.py`. Cross-process exclusion (any
of {1, 2} ↔ 3) is handled by git's own `.git/index.lock`. **Don't add a
fourth committer without first introducing an explicit mutex** — the
current correctness argument relies on these three being the only
writers.
