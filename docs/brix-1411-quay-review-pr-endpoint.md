# BRIX-1411: Quay PR Review HTTP Ingress

Hermes exposes an authenticated gateway API endpoint for CI-triggered Quay
PR review enrollment:

```http
POST /quay/review-pr
Authorization: Bearer <QUAY_REVIEW_PR_TOKEN>
Content-Type: application/json
```

Payload:

```json
{
  "repository": "InverterNetwork/hermes-agent",
  "pull_request": 104,
  "head_sha": "abc123",
  "event": "synchronize",
  "delivery_id": "123456789",
  "tags": ["task-feature"]
}
```

The gateway runs:

```sh
/usr/local/bin/quay-as-hermes review-pr --pr InverterNetwork/hermes-agent:104 --head-sha abc123 --tag task-feature
```

`QUAY_DATA_DIR` is pinned to `<HERMES_HOME>/quay` by default. The command
defaults to `/usr/local/bin/quay-as-hermes`, which loads Quay adapter env and
mints the GitHub App token the same way ad-hoc operator invocations do.

## Configuration

Stage the shared token outside source-controlled values:

```sh
sudo ./stage-secrets.sh
```

This writes `QUAY_REVIEW_PR_TOKEN` to `<HERMES_HOME>/auth/hermes.env`, which is
loaded by `hermes-gateway.service`.

The endpoint is served by the `api_server` gateway platform. Enable it in
`config.yaml` or with `API_SERVER_ENABLED=true`. If binding to a
network-accessible host, also set `API_SERVER_KEY`; the API server refuses to
start without a general API key in that mode because it exposes other API
routes besides `/quay/review-pr`.

Optional runtime overrides:

* `QUAY_REVIEW_PR_COMMAND` - command to exec, default
  `/usr/local/bin/quay-as-hermes`.
* `QUAY_REVIEW_PR_TIMEOUT_SECONDS` - command timeout, default `60`.
* `QUAY_DATA_DIR` - Quay data directory, default `<HERMES_HOME>/quay`.

## GitHub Actions Example

```yaml
name: quay-review

on:
  pull_request:
    types: [opened, synchronize, reopened]

jobs:
  enroll:
    runs-on: ubuntu-latest
    steps:
      - name: Enroll PR for Quay review
        env:
          QUAY_REVIEW_URL: ${{ secrets.QUAY_REVIEW_URL }}
          QUAY_REVIEW_PR_TOKEN: ${{ secrets.QUAY_REVIEW_PR_TOKEN }}
          PR_NUMBER: ${{ github.event.pull_request.number }}
          HEAD_SHA: ${{ github.event.pull_request.head.sha }}
          EVENT_ACTION: ${{ github.event.action }}
          DELIVERY_ID: ${{ github.run_id }}
        run: |
          curl -fsS -X POST "$QUAY_REVIEW_URL/quay/review-pr" \
            -H "Authorization: Bearer $QUAY_REVIEW_PR_TOKEN" \
            -H "Content-Type: application/json" \
            --data "$(jq -n \
              --arg repository "$GITHUB_REPOSITORY" \
              --arg head_sha "$HEAD_SHA" \
              --arg event "$EVENT_ACTION" \
              --arg delivery_id "$DELIVERY_ID" \
              --argjson pull_request "$PR_NUMBER" \
              '{repository:$repository,pull_request:$pull_request,head_sha:$head_sha,event:$event,delivery_id:$delivery_id,tags:["task-feature"]}')"
```
