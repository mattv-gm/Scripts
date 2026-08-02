#!/usr/bin/env python3
"""Pull audit events from the Matillion (Maia) DPC API within a time range.

Auth: OAuth2 client_credentials against https://id.core.matillion.com/oauth/dpc/token
Data: GET /v1/events on the region's DPC base URL, paginated.

Credentials are read from environment variables:
    MATILLION_CLIENT_ID
    MATILLION_CLIENT_SECRET

Example:
    python fetch_audit_events.py \
        --from 2026-07-01T00:00:00.000Z \
        --to 2026-07-29T00:00:00.000Z \
        --region us1 \
        --output audit_events.json
"""

import argparse
import json
import os
import socket
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

# us1.api.matillion.com resolves to both IPv4 and IPv6 (it's behind Cloudflare).
# If the client machine prefers IPv6, outbound requests land on an address that
# an org's IPv4-only IP allowlist rejects with 403 "IP not allowed for any
# organization" -- even though the same allowlist entry is correct. Forcing
# IPv4 keeps the egress IP consistent with whatever address was allowlisted.
_orig_getaddrinfo = socket.getaddrinfo


def _ipv4_only_getaddrinfo(host, port, family=0, type=0, proto=0, flags=0):
    return _orig_getaddrinfo(host, port, socket.AF_INET, type, proto, flags)


socket.getaddrinfo = _ipv4_only_getaddrinfo

TOKEN_URL = "https://id.core.matillion.com/oauth/dpc/token"
BASE_URLS = {
    "us1": "https://us1.api.matillion.com/dpc",
    "eu1": "https://eu1.api.matillion.com/dpc",
}
PAGE_SIZE = 100


def get_access_token(client_id, client_secret):
    body = urllib.parse.urlencode(
        {
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "audience": "https://api.matillion.com",
        }
    ).encode()
    req = urllib.request.Request(
        TOKEN_URL,
        data=body,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )
    with urllib.request.urlopen(req) as resp:
        payload = json.load(resp)
    return payload["access_token"], payload["expires_in"]


def fetch_events(base_url, token, from_ts, to_ts):
    results = []
    page = 0
    token_issued_at = time.monotonic()
    token_ttl = 1800

    while True:
        if time.monotonic() - token_issued_at > token_ttl - 60:
            raise RuntimeError(
                "Access token is about to expire mid-fetch; re-run with a "
                "narrower time range or add token-refresh logic."
            )

        query = urllib.parse.urlencode(
            {"from": from_ts, "to": to_ts, "page": page, "size": PAGE_SIZE}
        )
        url = f"{base_url}/v1/events?{query}"
        req = urllib.request.Request(
            url, headers={"Authorization": f"Bearer {token}"}, method="GET"
        )
        try:
            with urllib.request.urlopen(req) as resp:
                data = json.load(resp)
        except urllib.error.HTTPError as e:
            sys.exit(f"Request failed ({e.code}): {e.read().decode(errors='replace')}")

        batch = data.get("results", [])
        results.extend(batch)
        total = data.get("total", len(results))
        print(f"Fetched page {page}: {len(batch)} events ({len(results)}/{total})")

        if not batch or len(results) >= total:
            break
        page += 1

    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--from", dest="from_ts", required=True, help="ISO 8601 start time, e.g. 2026-07-01T00:00:00.000Z")
    parser.add_argument("--to", dest="to_ts", required=True, help="ISO 8601 end time, e.g. 2026-07-29T00:00:00.000Z")
    parser.add_argument("--region", choices=BASE_URLS.keys(), default="us1")
    parser.add_argument("--output", default="audit_events.json", help="Output JSON file path")
    args = parser.parse_args()

    client_id = os.environ.get("MATILLION_CLIENT_ID")
    client_secret = os.environ.get("MATILLION_CLIENT_SECRET")
    if not client_id or not client_secret:
        sys.exit(
            "Set MATILLION_CLIENT_ID and MATILLION_CLIENT_SECRET environment "
            "variables before running this script."
        )

    token, _ = get_access_token(client_id, client_secret)
    events = fetch_events(BASE_URLS[args.region], token, args.from_ts, args.to_ts)

    with open(args.output, "w") as f:
        json.dump(events, f, indent=2)

    print(f"Wrote {len(events)} events to {args.output}")


if __name__ == "__main__":
    main()
