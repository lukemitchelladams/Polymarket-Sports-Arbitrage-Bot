#!/usr/bin/env python3
"""
Un-resolve markets that were incorrectly marked resolved.
Keeps Rockets/finished games resolved, restores upcoming/active games.
"""
import json, time
from pathlib import Path

LEDGER = Path.home() / "polycopy" / "ledger.json"
data = json.load(open(LEDGER))

# Games we KNOW are finished — keep resolved
keep_resolved = ["rockets", "call of duty: vancouver"]

restored = []
for mid, d in data.items():
    if not d.get("resolved"):
        continue
    title = d.get("title", "").lower()
    if any(k in title for k in keep_resolved):
        continue  # keep resolved
    # Restore — game may not have started yet
    d["resolved"] = False
    d["stagnant"] = False
    d["last_trade"] = time.time()  # reset age so it doesn't immediately re-resolve
    restored.append(d.get("title","")[:60])

json.dump(data, open(LEDGER, 'w'), indent=2)
print(f"Restored {len(restored)} incorrectly resolved markets:")
for t in restored:
    print(f"  {t}")
