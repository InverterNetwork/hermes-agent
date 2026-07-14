# Dev NAV Sepolia Cron

The dev NAV Sepolia publisher is Hermes host state, not application
infrastructure. Checking the installer into the repo is not enough: it must be
run on the live Hermes host so the scheduler can see the materialized runtime
artifacts:

- `$HERMES_HOME/scripts/publish-dev-nav-sepolia.sh`
- `$HERMES_HOME/cron/jobs.json` entry named `dev-nav-sepolia-daily-publish`

BRIX-1626 added the installer, but the live host still had no matching cron job
because the host-state installation step was not enforced after deploy. The
current runbook closes that gap by making install, host-state verification, and
an immediate publish verification explicit deployment steps.

## Deployment

Run the installer on the Hermes host after deploying this Hermes Agent version:

```bash
python3 scripts/install_dev_nav_cron.py
```

Then verify the live host state:

```bash
python3 scripts/install_dev_nav_cron.py --verify
hermes cron list
```

`hermes cron list` must show exactly one enabled job named
`dev-nav-sepolia-daily-publish`. The installer also removes duplicate jobs with
that same owned name so the dev oracle is not published twice per day.

To prove the end-to-end write path after deployment, execute the installed cron
script once through the same materialized host artifact:

```bash
python3 scripts/install_dev_nav_cron.py --verify --run-now
```

The success line includes:

- `oracle=0x3dBe8d456198B63d46703bbf8f46778B2922c825`
- `chainId=11155111`
- `timestamp=<published lastNavUpdate()>`
- `previousTimestamp=<lastNavUpdate() before publish>`
- `tx=<transaction hash>` when `cast send --json` returns one

After the run, independently read `lastNavUpdate()` from the live oracle and
confirm it matches the published timestamp:

```bash
cast call 0x3dBe8d456198B63d46703bbf8f46778B2922c825 \
  "lastNavUpdate()(uint256)" \
  --rpc-url "$SEPOLIA_RPC_URL"
```

The job is a `no_agent` script-only cron scheduled at `30 6 * * *` UTC, which is
09:30 Europe/Istanbul and before the `NAV_STALE` warning window. No LLM is in
the write path.

The script validates the live dev source-of-truth row before writing:

- DynamoDB table: `main-dev`
- Key: `pk=FUND#DLF`, `sk=NET#11155111`
- Expected `navOracleAddress`: `0x3dBe8d456198B63d46703bbf8f46778B2922c825`

Required host tools and secrets:

- `aws` with read access to the DynamoDB row above
- `cast` from Foundry
- `SEPOLIA_RPC_URL`
- `DEV_NAV_PUBLISH_ACCOUNT` for a Foundry keystore account, or
  `DEV_NAV_PUBLISH_KEYSTORE` for an explicit keystore file/folder
- `DEV_NAV_PUBLISH_PASSWORD_FILE`
- Either `DEV_NAV_PRICE_WEI` or `PROD_ETH_RPC_URL`/`MAINNET_RPC_URL`/`ETH_RPC_URL`

The publish signer must be a dedicated dev NAV credential. The script rejects
raw private-key environment variables and does not pass signing secrets on the
`cast send` command line. `DEV_NAV_PUBLISH_PASSWORD_FILE` is required so
`cast send` fails fast instead of prompting interactively in cron.

When `DEV_NAV_PRICE_WEI` is not set, the script reads the prod oracle
`latestReportedPrice()` from
`0x60A91420c98c8461e20E9A6DDA555E21c7BDbfFe` and publishes that value to dev
with `setNav(uint256,uint256)`.

After sending the transaction, the script verifies dev `latestReportedPrice()`
and `lastNavUpdate()` and prints a single success line including the oracle,
chain id, price, timestamp, previous timestamp, price source, and transaction
hash when available. Failures are printed to stderr and delivered by Hermes
cron as a failed no-agent job.
