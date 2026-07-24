"""Behaviour tests for ``ops/atlas-source-sync-runner``.

The runner drives ``atlas sync source <name>`` for each configured source. The
regression these tests guard: a failure (or transient KB write-lock contention)
on ONE source must never abort the later sources — that is exactly how a lock
collision on the first source (emusd-docs) once silently prevented the Slack
FULL reconciliation from ever running. The runner isolates each source, retries
transient lock contention, emits a deterministic per-source summary, and exits
non-zero iff a source genuinely failed (so OnFailure= still alerts).

The runner is exercised as the real bash script against stub ``atlas-as-hermes``
and token-helper executables, so the isolation/retry/exit semantics are tested
end to end without touching Atlas, the KB, or the network.
"""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
RUNNER = REPO_ROOT / "ops" / "atlas-source-sync-runner"

# A stub `atlas-as-hermes`. Behaviour per source is driven by env:
#   STUB_FAIL        — comma list: deterministic failure (exit 1), no lock marker
#   STUB_LOCK_ONCE   — comma list: emit lock marker + exit 1 on attempt 1 only
#   STUB_LOCK_ALWAYS — comma list: always emit lock marker + exit 1
# A per-source attempt counter under STUB_STATE lets "once" flip to success.
ATLAS_STUB = r"""#!/usr/bin/env bash
# invoked as: sync source <name> [--full ...]
name="$3"
sd="$STUB_STATE"
cf="$sd/attempts.$name"
n=$(( $(cat "$cf" 2>/dev/null || echo 0) + 1 ))
echo "$n" > "$cf"
echo "[atlas-stub] sync source $name attempt=$n args=$*"
case ",${STUB_LOCK_ALWAYS:-}," in *",$name,"*) echo '{"reason":"kb_write_lock_held"}'; exit 1;; esac
case ",${STUB_LOCK_ONCE:-}," in *",$name,"*) if [ "$n" -eq 1 ]; then echo '{"reason":"kb_write_lock_held"}'; exit 1; fi;; esac
case ",${STUB_FAIL:-}," in *",$name,"*) echo "[atlas-stub] $name deterministic failure"; exit "${STUB_EXIT:-1}";; esac
echo '{"event":"summary","success":true}'
exit 0
"""

TOKEN_STUB = "#!/usr/bin/env bash\necho faketoken\n"


def _write_exec(path: Path, content: str) -> Path:
    path.write_text(content, encoding="utf-8")
    path.chmod(0o755)
    return path


@pytest.fixture
def run_runner(tmp_path: Path):
    if shutil.which("bash") is None:  # pragma: no cover - CI always has bash
        pytest.skip("bash not available")
    state = tmp_path / "state"
    state.mkdir()
    atlas_stub = _write_exec(tmp_path / "atlas-as-hermes", ATLAS_STUB)
    token_py = _write_exec(tmp_path / "token.sh", TOKEN_STUB)
    token_helper = tmp_path / "helper.py"
    token_helper.write_text("# stub\n", encoding="utf-8")
    reader_cfg = tmp_path / "atlas-source-reader.env"
    reader_cfg.write_text("# stub\n", encoding="utf-8")

    def _run(sources: str, extra_env: dict[str, str] | None = None, args: list[str] | None = None):
        env = {
            **os.environ,
            "HERMES_HOME": str(tmp_path),
            "HERMES_TOKEN_PYTHON": str(token_py),
            "HERMES_TOKEN_HELPER": str(token_helper),
            "HERMES_ATLAS_SOURCE_READER_GH_CONFIG": str(reader_cfg),
            "ATLAS_AS_HERMES_BIN": str(atlas_stub),
            "ATLAS_SYNC_SOURCE_NAMES": sources,
            "STUB_STATE": str(state),
            # Make retry cheap and deterministic in tests.
            "ATLAS_SYNC_LOCK_RETRY_DELAY": "0",
        }
        if extra_env:
            env.update(extra_env)
        return subprocess.run(
            ["bash", str(RUNNER), *(args or [])],
            capture_output=True,
            text=True,
            env=env,
            check=False,
        )

    return _run


def test_all_sources_succeed_exit_zero(run_runner) -> None:
    r = run_runner("a,b,c")
    assert r.returncode == 0, r.stderr
    assert "source-sync summary: a=ok(attempts=1) b=ok(attempts=1) c=ok(attempts=1)" in r.stderr


def test_one_source_failure_does_not_abort_later_sources(run_runner) -> None:
    # b fails deterministically; a (before) and c (AFTER) must both still run.
    r = run_runner("a,b,c", extra_env={"STUB_FAIL": "b"})
    assert "a=ok(attempts=1)" in r.stderr
    assert "b=FAILED(attempts=1,exit=1)" in r.stderr
    assert "c=ok(attempts=1)" in r.stderr, "later source c must run despite b failing"
    # A genuine failure yields a non-zero aggregate exit → OnFailure= fires.
    assert r.returncode != 0


def test_non_lock_failure_is_not_retried(run_runner) -> None:
    # A deterministic (non-lock) failure must fail on the first attempt, not spin.
    r = run_runner("b", extra_env={"STUB_FAIL": "b"})
    assert "b=FAILED(attempts=1,exit=1)" in r.stderr
    assert r.returncode != 0


def test_transient_lock_is_retried_then_succeeds(run_runner) -> None:
    # b hits the lock marker on attempt 1, then succeeds → retried, run succeeds.
    r = run_runner("a,b,c", extra_env={"STUB_LOCK_ONCE": "b"})
    assert "b=ok(attempts=2)" in r.stderr, r.stderr
    assert r.returncode == 0
    assert "KB write-lock contention" in r.stderr


def test_persistent_lock_exhausts_bounded_retries_then_fails(run_runner) -> None:
    # b always reports the lock; with 2 retries it is attempted 3 times, then fails.
    r = run_runner(
        "a,b",
        extra_env={"STUB_LOCK_ALWAYS": "b", "ATLAS_SYNC_LOCK_RETRIES": "2"},
    )
    assert "a=ok(attempts=1)" in r.stderr
    assert "b=FAILED(attempts=3,exit=1)" in r.stderr, r.stderr
    assert r.returncode != 0


def test_lock_contention_on_first_source_still_runs_slack_last(run_runner) -> None:
    # Reproduces the production incident: the FIRST source hits the lock. It must
    # not abort the run — the later (slack) source must still reconcile.
    r = run_runner(
        "emusd-docs,brix-product-docs,slack-channels",
        extra_env={"STUB_LOCK_ONCE": "emusd-docs"},
    )
    assert "emusd-docs=ok(attempts=2)" in r.stderr
    assert "slack-channels=ok(attempts=1)" in r.stderr, (
        "slack full reconciliation must run even when an earlier source hit the lock"
    )
    assert r.returncode == 0


def test_summary_preserves_source_order(run_runner) -> None:
    r = run_runner("c,a,b")
    summary = next(
        (ln for ln in r.stderr.splitlines() if "source-sync summary:" in ln), ""
    )
    assert summary, r.stderr
    assert summary.index("c=") < summary.index("a=") < summary.index("b=")


def test_flags_are_forwarded_to_each_source(run_runner) -> None:
    # The daily-full unit passes --full through to every source.
    r = run_runner("a,b", args=["--full"])
    assert r.returncode == 0
    assert "args=sync source a --full" in r.stderr
    assert "args=sync source b --full" in r.stderr


def test_runs_with_default_lock_knobs_unset(run_runner) -> None:
    # Rollback-safety: the new lock-retry knobs must default on their own, so
    # reverting any env/config override (or an older /etc/default) cannot break
    # the runner. Drop the fixture's ATLAS_SYNC_LOCK_RETRY_DELAY and run a clean
    # success path (no retry → no real sleep).
    r = run_runner("a,b,c", extra_env={"ATLAS_SYNC_LOCK_RETRY_DELAY": ""})
    assert r.returncode == 0, r.stderr
    assert "a=ok(attempts=1) b=ok(attempts=1) c=ok(attempts=1)" in r.stderr


def test_single_source_backward_compatible(run_runner) -> None:
    # The historical default was a single source; the isolated loop must still
    # handle one source cleanly (a rollback of source_names is harmless).
    r = run_runner("emusd-docs")
    assert r.returncode == 0
    assert "source-sync summary: emusd-docs=ok(attempts=1)" in r.stderr


def test_malformed_retry_count_is_coerced_not_fatal(run_runner) -> None:
    # A non-integer knob must NOT abort the runner under set -u (arithmetic on a
    # bad value would raise "unbound variable"). It is coerced; sync still runs.
    r = run_runner(
        "a,b,c",
        extra_env={"ATLAS_SYNC_LOCK_RETRIES": "banana", "STUB_LOCK_ONCE": "b"},
    )
    assert r.returncode == 0, r.stderr
    assert "is not a 1-3 digit integer; using 3" in r.stderr
    assert "b=ok(attempts=2)" in r.stderr  # retry still worked after coercion


def test_oversized_retry_count_over_max_is_clamped(run_runner) -> None:
    # A 3-digit value above the max is clamped to the max (10), not spun forever.
    # With STUB_LOCK_ALWAYS the source fails after 1 + 10 = 11 attempts.
    r = run_runner(
        "b",
        extra_env={"ATLAS_SYNC_LOCK_RETRIES": "099", "STUB_LOCK_ALWAYS": "b"},
    )
    assert "exceeds max 10; clamping to 10" in r.stderr
    assert "b=FAILED(attempts=11,exit=1)" in r.stderr, r.stderr
    assert r.returncode != 0


def test_too_many_digits_retry_count_is_coerced(run_runner) -> None:
    # >3 digits (overflow guard) is rejected → default 3 → 4 attempts, not 1000.
    r = run_runner(
        "b",
        extra_env={"ATLAS_SYNC_LOCK_RETRIES": "1000", "STUB_LOCK_ALWAYS": "b"},
    )
    assert "is not a 1-3 digit integer; using 3" in r.stderr
    assert "b=FAILED(attempts=4,exit=1)" in r.stderr, r.stderr
    assert r.returncode != 0


def test_leading_zero_retry_count_is_decimal_not_octal(run_runner) -> None:
    # "012" must normalize to decimal 12 (then clamp to 10), NOT octal 10 — and it
    # must never raise a bash arithmetic/octal error. 1 + 10 = 11 attempts.
    r = run_runner(
        "b",
        extra_env={"ATLAS_SYNC_LOCK_RETRIES": "012", "STUB_LOCK_ALWAYS": "b"},
    )
    assert "b=FAILED(attempts=11,exit=1)" in r.stderr, r.stderr
    assert "value too great for base" not in r.stderr  # no octal parse error
    assert r.returncode != 0


def test_malformed_delay_is_coerced(run_runner) -> None:
    # A bad delay must be coerced (no set -u abort). Use an all-success run so the
    # coerced delay is never actually slept on — we only assert the coercion.
    r = run_runner("a,b,c", extra_env={"ATLAS_SYNC_LOCK_RETRY_DELAY": "banana"})
    assert r.returncode == 0, r.stderr
    assert "ATLAS_SYNC_LOCK_RETRY_DELAY='banana' is not a 1-3 digit integer; using 20" in r.stderr


def test_oversized_delay_over_max_is_clamped(run_runner) -> None:
    # A 4-digit delay (0454) is rejected as too many digits → default 20; a 3-digit
    # over-max delay would clamp to 300. Either way it can never become 454s.
    r = run_runner("a", extra_env={"ATLAS_SYNC_LOCK_RETRY_DELAY": "0454"})
    assert r.returncode == 0, r.stderr
    assert "ATLAS_SYNC_LOCK_RETRY_DELAY='0454' is not a 1-3 digit integer; using 20" in r.stderr


def test_persistent_lock_on_first_source_still_runs_slack(run_runner) -> None:
    # The reported incident, worst case: the FIRST source is stuck on the lock for
    # good. It must exhaust its bounded retries, be recorded FAILED, and the later
    # Slack source must STILL reconcile. The run fails overall (so OnFailure fires)
    # but Slack was not silently skipped.
    r = run_runner(
        "emusd-docs,slack-channels",
        extra_env={"STUB_LOCK_ALWAYS": "emusd-docs", "ATLAS_SYNC_LOCK_RETRIES": "2"},
    )
    assert "emusd-docs=FAILED(attempts=3,exit=1)" in r.stderr
    assert "slack-channels=ok(attempts=1)" in r.stderr, (
        "Slack must reconcile even when the first source is permanently locked"
    )
    assert r.returncode != 0


def test_non_one_exit_code_is_recorded_and_not_retried(run_runner) -> None:
    # A non-lock failure with a non-1 exit code is recorded verbatim and not
    # retried (only lock contention retries).
    r = run_runner("a,b", extra_env={"STUB_FAIL": "b", "STUB_EXIT": "2"})
    assert "b=FAILED(attempts=1,exit=2)" in r.stderr, r.stderr
    assert "a=ok(attempts=1)" in r.stderr
    assert r.returncode != 0


def test_comma_only_source_list_runs_nothing(run_runner) -> None:
    # Empty entries (a comma-only / whitespace value) are skipped, so no source is
    # attempted and the run is a clean no-op. (A truly empty value falls back to
    # the historical single default source, which is covered elsewhere.)
    r = run_runner(",, ,")
    assert r.returncode == 0, r.stderr
    assert "running atlas sync source" not in r.stderr  # nothing attempted
    assert "source-sync summary:" in r.stderr
