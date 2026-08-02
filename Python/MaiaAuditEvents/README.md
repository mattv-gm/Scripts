# Maia (Matillion DPC) Audit Events

`fetch_audit_events.py` pulls audit events from Matillion's Data Productivity Cloud API for a given time range and writes them to a local JSON file.

## Setup

1. Get a **Client ID** and **Client Secret** for a Maia API credential (Administration → API Credentials in the Maia console). The secret is only shown once at creation.
2. Export them as environment variables:
   ```bash
   export MATILLION_CLIENT_ID=...
   export MATILLION_CLIENT_SECRET=...
   ```

No other dependencies are required — the script only uses the Python standard library.

## Usage

```bash
python fetch_audit_events.py \
  --from 2026-07-01T00:00:00.000Z \
  --to 2026-07-29T00:00:00.000Z \
  --region us1 \
  --output audit_events.json
```

| Flag | Required | Description |
|---|---|---|
| `--from` | yes | ISO 8601 start of the time range |
| `--to` | yes | ISO 8601 end of the time range |
| `--region` | no | `us1` (default) or `eu1` — must match the region your Maia account is hosted in |
| `--output` | no | Output JSON file path (default `audit_events.json`) |

## How it works

1. Exchanges the client ID/secret for a short-lived bearer token via `POST https://id.core.matillion.com/oauth/dpc/token` (`grant_type=client_credentials`).
2. Pages through `GET /v1/events` (100 events per page) on the region's DPC base URL until all events in the range are retrieved.
3. Writes the combined `results` array to `--output` as JSON.

**Tokens expire after 30 minutes and cannot be refreshed** — the script aborts if a fetch is still running near expiry rather than silently failing mid-page. For very large time ranges, narrow `--from`/`--to` and run the script multiple times.
