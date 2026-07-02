#!/usr/bin/env python3
"""Install and verify the dev NAV Sepolia publisher cron job.

This is intentionally an operator-run installer: Hermes cron jobs and scripts
live in host state under HERMES_HOME, not in the source checkout. Running this
script materializes the deterministic no-agent script payload and upserts the
daily cron record. ``--verify`` audits the live host state after deployment, and
``--run-now`` executes the same materialized script path used by cron.
"""

from __future__ import annotations

import argparse
import os
import stat
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hermes_constants import get_hermes_home


JOB_NAME = "dev-nav-sepolia-daily-publish"
JOB_SCHEDULE = "30 6 * * *"
SCRIPT_NAME = "publish-dev-nav-sepolia.sh"
DEV_NAV_ORACLE_ADDRESS = "0x3dBe8d456198B63d46703bbf8f46778B2922c825"


SCRIPT_PAYLOAD = r'''#!/usr/bin/env bash
set -euo pipefail

DEV_NAV_ORACLE_ADDRESS="${DEV_NAV_ORACLE_ADDRESS:-0x3dBe8d456198B63d46703bbf8f46778B2922c825}"
EXPECTED_DEV_NAV_ORACLE_ADDRESS="0x3dBe8d456198B63d46703bbf8f46778B2922c825"
DEV_NAV_DDB_TABLE="${DEV_NAV_DDB_TABLE:-main-dev}"
DEV_NAV_DDB_PK="${DEV_NAV_DDB_PK:-FUND#DLF}"
DEV_NAV_DDB_SK="${DEV_NAV_DDB_SK:-NET#11155111}"
PROD_NAV_ORACLE_ADDRESS="${PROD_NAV_ORACLE_ADDRESS:-0x60A91420c98c8461e20E9A6DDA555E21c7BDbfFe}"

HERMES_HOME="${HERMES_HOME:-$HOME/.hermes}"
if [ -f "$HERMES_HOME/.env" ]; then
  set -a
  # shellcheck disable=SC1091
  . "$HERMES_HOME/.env"
  set +a
fi

need_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "dev-nav-publish failed: required command '$1' is not on PATH" >&2
    exit 1
  fi
}

to_dec() {
  python3 - "$1" <<'PY'
import sys
value = sys.argv[1].strip()
if value.startswith(("0x", "0X")):
    print(int(value, 16))
else:
    print(int(value))
PY
}

need_cmd aws
need_cmd cast
need_cmd python3

sepolia_rpc_url="${SEPOLIA_RPC_URL:-${DEV_SEPOLIA_RPC_URL:-${ETH_SEPOLIA_RPC_URL:-}}}"
if [ -z "$sepolia_rpc_url" ]; then
  echo "dev-nav-publish failed: set SEPOLIA_RPC_URL, DEV_SEPOLIA_RPC_URL, or ETH_SEPOLIA_RPC_URL" >&2
  exit 1
fi

signer_args=()
if [ -n "${DEV_NAV_PUBLISH_KEYSTORE:-}" ]; then
  signer_args+=(--keystore "$DEV_NAV_PUBLISH_KEYSTORE")
elif [ -n "${DEV_NAV_PUBLISH_ACCOUNT:-}" ]; then
  signer_args+=(--account "$DEV_NAV_PUBLISH_ACCOUNT")
else
  echo "dev-nav-publish failed: set DEV_NAV_PUBLISH_ACCOUNT or DEV_NAV_PUBLISH_KEYSTORE" >&2
  exit 1
fi

if [ -z "${DEV_NAV_PUBLISH_PASSWORD_FILE:-}" ]; then
  echo "dev-nav-publish failed: set DEV_NAV_PUBLISH_PASSWORD_FILE so cast cannot prompt interactively" >&2
  exit 1
fi
if [ ! -r "$DEV_NAV_PUBLISH_PASSWORD_FILE" ]; then
  echo "dev-nav-publish failed: DEV_NAV_PUBLISH_PASSWORD_FILE is not readable: $DEV_NAV_PUBLISH_PASSWORD_FILE" >&2
  exit 1
fi
signer_args+=(--password-file "$DEV_NAV_PUBLISH_PASSWORD_FILE")

if [ -n "${DEV_NAV_PUBLISH_PRIVATE_KEY+x}${NAV_PUBLISH_PRIVATE_KEY+x}" ]; then
  echo "dev-nav-publish failed: raw private-key env vars are not accepted; use a Foundry keystore account" >&2
  exit 1
fi

if [ "$DEV_NAV_ORACLE_ADDRESS" != "$EXPECTED_DEV_NAV_ORACLE_ADDRESS" ]; then
  echo "dev-nav-publish failed: DEV_NAV_ORACLE_ADDRESS must be $EXPECTED_DEV_NAV_ORACLE_ADDRESS" >&2
  exit 1
fi

ddb_oracle="$(
  aws dynamodb get-item \
    --table-name "$DEV_NAV_DDB_TABLE" \
    --key "{\"pk\":{\"S\":\"$DEV_NAV_DDB_PK\"},\"sk\":{\"S\":\"$DEV_NAV_DDB_SK\"}}" \
    --projection-expression navOracleAddress \
    --query 'Item.navOracleAddress.S' \
    --output text
)"

if [ "$ddb_oracle" != "$EXPECTED_DEV_NAV_ORACLE_ADDRESS" ]; then
  echo "dev-nav-publish failed: DynamoDB $DEV_NAV_DDB_TABLE $DEV_NAV_DDB_PK/$DEV_NAV_DDB_SK has navOracleAddress=$ddb_oracle, expected $EXPECTED_DEV_NAV_ORACLE_ADDRESS" >&2
  exit 1
fi

nav_price="${DEV_NAV_PRICE_WEI:-}"
price_source="DEV_NAV_PRICE_WEI"
if [ -z "$nav_price" ]; then
  prod_rpc_url="${PROD_ETH_RPC_URL:-${MAINNET_RPC_URL:-${ETH_RPC_URL:-}}}"
  if [ -z "$prod_rpc_url" ]; then
    echo "dev-nav-publish failed: set DEV_NAV_PRICE_WEI or PROD_ETH_RPC_URL/MAINNET_RPC_URL/ETH_RPC_URL" >&2
    exit 1
  fi
  nav_price="$(to_dec "$(cast call "$PROD_NAV_ORACLE_ADDRESS" "latestReportedPrice()(uint256)" --rpc-url "$prod_rpc_url")")"
  price_source="prod:$PROD_NAV_ORACLE_ADDRESS latestReportedPrice()"
fi

if [ "$nav_price" -le 0 ]; then
  echo "dev-nav-publish failed: nav price must be positive, got $nav_price" >&2
  exit 1
fi

nav_timestamp="${DEV_NAV_TIMESTAMP:-$(date +%s)}"
if [ "$nav_timestamp" -gt "$(date +%s)" ]; then
  echo "dev-nav-publish failed: nav timestamp $nav_timestamp is in the future" >&2
  exit 1
fi

before_ts="$(to_dec "$(cast call "$DEV_NAV_ORACLE_ADDRESS" "lastNavUpdate()(uint256)" --rpc-url "$sepolia_rpc_url")")"

send_json="$(
  cast send "$DEV_NAV_ORACLE_ADDRESS" \
    "setNav(uint256,uint256)" "$nav_price" "$nav_timestamp" \
    --rpc-url "$sepolia_rpc_url" \
    "${signer_args[@]}" \
    --json
)"

tx_hash="$(python3 -c 'import json,sys; data=json.load(sys.stdin); print(data.get("transactionHash") or data.get("hash") or "")' <<<"$send_json" 2>/dev/null || true)"

after_price="$(to_dec "$(cast call "$DEV_NAV_ORACLE_ADDRESS" "latestReportedPrice()(uint256)" --rpc-url "$sepolia_rpc_url")")"
after_ts="$(to_dec "$(cast call "$DEV_NAV_ORACLE_ADDRESS" "lastNavUpdate()(uint256)" --rpc-url "$sepolia_rpc_url")")"

if [ "$after_price" != "$nav_price" ] || [ "$after_ts" != "$nav_timestamp" ]; then
  echo "dev-nav-publish failed: verification mismatch price=$after_price timestamp=$after_ts expected_price=$nav_price expected_timestamp=$nav_timestamp" >&2
  exit 1
fi

printf 'dev-nav-publish ok oracle=%s chainId=11155111 priceWei=%s timestamp=%s previousTimestamp=%s priceSource="%s"' \
  "$DEV_NAV_ORACLE_ADDRESS" "$nav_price" "$nav_timestamp" "$before_ts" "$price_source"
if [ -n "$tx_hash" ]; then
  printf ' tx=%s' "$tx_hash"
fi
printf '\n'
'''


class VerificationError(RuntimeError):
    """Raised when host cron state does not match the expected NAV job."""


def _script_path() -> Path:
    return get_hermes_home() / "scripts" / SCRIPT_NAME


def _write_script() -> Path:
    scripts_dir = get_hermes_home() / "scripts"
    scripts_dir.mkdir(parents=True, exist_ok=True)

    script_path = scripts_dir / SCRIPT_NAME
    script_path.write_text(SCRIPT_PAYLOAD, encoding="utf-8")
    script_path.chmod(stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)
    return script_path


def _matching_jobs():
    from cron.jobs import list_jobs

    return [job for job in list_jobs(include_disabled=True) if job.get("name") == JOB_NAME]


def _verify_job_shape(job: dict) -> list[str]:
    errors: list[str] = []
    schedule = job.get("schedule") or {}
    checks = {
        "schedule.kind": schedule.get("kind") == "cron",
        "schedule.expr": schedule.get("expr") == JOB_SCHEDULE,
        "schedule_display": job.get("schedule_display") == JOB_SCHEDULE,
        "script": job.get("script") == SCRIPT_NAME,
        "no_agent": job.get("no_agent") is True,
        "enabled": job.get("enabled") is True,
        "state": job.get("state") == "scheduled",
        "prompt": job.get("prompt") in {None, ""},
    }
    for field, ok in checks.items():
        if not ok:
            errors.append(f"{field} drifted")
    return errors


def verify_installed() -> dict:
    """Verify the materialized host state for the dev NAV cron job."""

    script_path = _script_path()
    errors: list[str] = []
    if not script_path.is_file():
        errors.append(f"missing script: {script_path}")
    else:
        if script_path.read_text(encoding="utf-8") != SCRIPT_PAYLOAD:
            errors.append(f"script payload drifted: {script_path}")
        mode = script_path.stat().st_mode
        expected_mode = stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR
        if stat.S_IMODE(mode) != expected_mode:
            errors.append(f"script mode is {stat.S_IMODE(mode):03o}, expected 700")

    jobs = _matching_jobs()
    if len(jobs) != 1:
        errors.append(f"expected exactly one {JOB_NAME!r} job, found {len(jobs)}")
    elif job_errors := _verify_job_shape(jobs[0]):
        errors.extend(job_errors)

    if errors:
        raise VerificationError("; ".join(errors))

    return {
        "job": jobs[0],
        "script": script_path,
    }


def install() -> None:
    script_path = _write_script()

    from cron.jobs import create_job, remove_job, update_job

    matches = _matching_jobs()
    existing = matches[0] if matches else None
    if existing:
        job = update_job(
            existing["id"],
            {
                "schedule": JOB_SCHEDULE,
                "schedule_display": JOB_SCHEDULE,
                "script": SCRIPT_NAME,
                "no_agent": True,
                "prompt": None,
                "deliver": existing.get("deliver") or "local",
                "enabled": True,
                "state": "scheduled",
                "paused_at": None,
                "paused_reason": None,
            },
        )
        for duplicate in matches[1:]:
            remove_job(duplicate["id"])
        action = "updated"
    else:
        job = create_job(
            prompt=None,
            schedule=JOB_SCHEDULE,
            name=JOB_NAME,
            script=SCRIPT_NAME,
            no_agent=True,
            deliver="local",
        )
        action = "created"

    verified = verify_installed()
    print(f"{action} cron job {job['id']}: {JOB_NAME}")
    print(f"script: {script_path}")
    print(f"schedule: {JOB_SCHEDULE} UTC (09:30 Europe/Istanbul)")
    print(f"oracle: {DEV_NAV_ORACLE_ADDRESS}")
    print("mode: no_agent script-only")
    print(f"verified: cron job {verified['job']['id']} and script payload match expected host state")


def run_now() -> None:
    """Execute the installed script once and stream its verification output."""

    verified = verify_installed()
    script_path = verified["script"]
    env = os.environ.copy()
    env.setdefault("HERMES_HOME", str(get_hermes_home()))
    result = subprocess.run(
        ["bash", str(script_path)],
        cwd=str(get_hermes_home()),
        env=env,
        text=True,
        capture_output=True,
        timeout=int(os.environ.get("DEV_NAV_RUN_NOW_TIMEOUT", "180")),
        check=False,
    )
    if result.stdout:
        print(result.stdout, end="")
    if result.stderr:
        print(result.stderr, end="", file=sys.stderr)
    if result.returncode != 0:
        raise SystemExit(result.returncode)


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Install, verify, and optionally execute the host-managed dev "
            "Sepolia NAV publisher cron job."
        )
    )
    parser.add_argument(
        "--verify",
        action="store_true",
        help="Only verify that the live HERMES_HOME script and cron job are installed correctly.",
    )
    parser.add_argument(
        "--run-now",
        action="store_true",
        help="After install/verify, execute the installed script once to publish and verify on-chain.",
    )
    return parser.parse_args(argv)


if __name__ == "__main__":
    os.environ.setdefault("HERMES_IGNORE_RULES", "1")
    args = _parse_args(sys.argv[1:])
    if args.verify:
        try:
            state = verify_installed()
        except VerificationError as exc:
            print(f"dev NAV cron verification failed: {exc}", file=sys.stderr)
            raise SystemExit(1) from exc
        print(f"dev NAV cron verified: job={state['job']['id']} script={state['script']}")
    else:
        install()
    if args.run_now:
        run_now()
