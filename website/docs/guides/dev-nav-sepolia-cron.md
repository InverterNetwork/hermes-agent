# Dev NAV Sepolia Cron

BRIX-1626 is deployed as Hermes host state, not application infrastructure.
The repo ships an installer that writes both required runtime artifacts:

- `$HERMES_HOME/scripts/publish-dev-nav-sepolia.sh`
- `$HERMES_HOME/cron/jobs.json` entry named `dev-nav-sepolia-daily-publish`

Run the installer on the Hermes host after deploying this Hermes Agent version:

```bash
python3 scripts/install_dev_nav_cron.py
```

The job is a `no_agent` script-only cron scheduled at `30 6 * * *` UTC, which
is 09:30 Europe/Istanbul and before the `NAV_STALE` warning window.

The script validates the live dev source-of-truth row before writing:

- DynamoDB table: `main-dev`
- Key: `pk=FUND#DLF`, `sk=NET#11155111`
- Expected `navOracleAddress`: `0x3dBe8d456198B63d46703bbf8f46778B2922c825`

Required host tools and secrets:

- `aws` with read access to the DynamoDB row above
- `cast` from Foundry
- `SEPOLIA_RPC_URL`
- `DEV_NAV_PUBLISH_PRIVATE_KEY` or `NAV_PUBLISH_PRIVATE_KEY`
- Either `DEV_NAV_PRICE_WEI` or `PROD_ETH_RPC_URL`/`MAINNET_RPC_URL`/`ETH_RPC_URL`

When `DEV_NAV_PRICE_WEI` is not set, the script reads the prod oracle
`latestReportedPrice()` from
`0x60A91420c98c8461e20E9A6DDA555E21c7BDbfFe` and publishes that value to dev
with `setNav(uint256,uint256)`.

After sending the transaction, the script verifies dev `latestReportedPrice()`
and `lastNavUpdate()` and prints a single success line including the oracle,
chain id, price, timestamp, previous timestamp, price source, and transaction
hash when available. Failures are printed to stderr and delivered by Hermes
cron as a failed no-agent job.
