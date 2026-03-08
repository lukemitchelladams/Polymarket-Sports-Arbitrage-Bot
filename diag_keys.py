#!/usr/bin/env python3
"""Quick diagnostic for Polymarket signing."""
import os

with open("keys.env") as f:
    for line in f:
        line = line.strip()
        if "=" in line and not line.startswith("#"):
            k, v = line.split("=", 1)
            os.environ[k.strip()] = v.strip()

pk = os.getenv("PK", "")
pw = os.getenv("PROXY_WALLET", "")
funder = os.getenv("FUNDER", "")
print(f"PK set: {bool(pk)} (len={len(pk)})")
print(f"PK starts with 0x: {pk.startswith('0x')}")
print(f"PROXY_WALLET: {pw}")
print(f"FUNDER: {funder or '(not set)'}")

from py_clob_client import ClobClient

for sig_type in [0, 1, 2]:
    try:
        kwargs = {"key": pk, "chain_id": 137, "signature_type": sig_type}
        if sig_type == 2 and pw:
            kwargs["funder"] = pw
        c = ClobClient("https://clob.polymarket.com", **kwargs)
        creds = c.create_or_derive_api_creds()
        c.set_api_creds(creds)
        print(f"sig_type={sig_type} creds OK")
    except Exception as e:
        print(f"sig_type={sig_type} FAILED: {e}")
