#!/usr/bin/env python3
"""Install the BRIX-1626 dev NAV Sepolia publisher cron job.

This is intentionally an operator-run installer: Hermes cron jobs and scripts
live in host state under HERMES_HOME, not in the source checkout. Running this
script materializes the deterministic no-agent script payload and upserts the
daily cron record.
"""

from __future__ import annotations

import os
import stat
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

private_key="${DEV_NAV_PUBLISH_PRIVATE_KEY:-${NAV_PUBLISH_PRIVATE_KEY:-${PRIVATE_KEY:-}}}"
if [ -z "$private_key" ]; then
  echo "dev-nav-publish failed: set DEV_NAV_PUBLISH_PRIVATE_KEY or NAV_PUBLISH_PRIVATE_KEY" >&2
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
    --private-key "$private_key" \
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


def install() -> None:
    hermes_home = get_hermes_home()
    scripts_dir = hermes_home / "scripts"
    scripts_dir.mkdir(parents=True, exist_ok=True)

    script_path = scripts_dir / SCRIPT_NAME
    script_path.write_text(SCRIPT_PAYLOAD, encoding="utf-8")
    script_path.chmod(stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)

    from cron.jobs import create_job, list_jobs, update_job

    existing = next((job for job in list_jobs(include_disabled=True) if job.get("name") == JOB_NAME), None)
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

    print(f"{action} cron job {job['id']}: {JOB_NAME}")
    print(f"script: {script_path}")
    print(f"schedule: {JOB_SCHEDULE} UTC (09:30 Europe/Istanbul)")
    print(f"oracle: {DEV_NAV_ORACLE_ADDRESS}")
    print("mode: no_agent script-only")


if __name__ == "__main__":
    os.environ.setdefault("HERMES_IGNORE_RULES", "1")
    install()
