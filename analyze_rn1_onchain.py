#!/usr/bin/env python3
"""
RN1 On-Chain Wallet Analyzer v2
════════════════════════════════
His wallet is a SMART CONTRACT on Polygon.
This version properly pulls internal transactions and contract interactions.

Usage:
  python3.11 analyze_rn1_onchain.py --api-key YOUR_KEY
"""

import os, sys, json, time, argparse
from datetime import datetime
from collections import Counter, defaultdict
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError

RN1_WALLET = "0x2005d16a84ceefa912d4e380cd32e7ff827875ea"
POLYGONSCAN_BASE = "https://api.polygonscan.com/api"

KNOWN_CONTRACTS = {
    "0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e": "CTF Exchange",
    "0x4d97dcd97ec945f40cf65f87097ace5ea0476045": "Neg Risk CTF Exchange",
    "0x2791bca1f2de4661ed88a30c99a7a9449aa84174": "USDC (PoS)",
    "0x3c499c542cef5e3811e1192ce70d8cc03d5c3359": "USDC Native",
    "0xd51d1aab39670a01e44bf8e25faba2f6f4cddc51": "Polymarket Proxy",
    "0x0000000000000000000000000000000000001010": "MATIC",
}

KNOWN_METHODS = {
    "0xa6dfcf86": "fillOrder",
    "0xe60bd2ba": "fillOrders (batch)",
    "0x0d0a2b37": "matchOrders",
    "0xd2539758": "cancelOrder",
    "0x095ea7b3": "approve",
    "0xa9059cbb": "transfer",
    "0x23b872dd": "transferFrom",
    "0x6a761202": "execTransaction (Safe)",
    "0x0c53c51c": "executeMetaTransaction",
    "0x2eb2c2d6": "safeBatchTransferFrom",
    "0xf242432a": "safeTransferFrom",
}


def api_call(params, api_key):
    params["apikey"] = api_key
    query = "&".join(f"{k}={v}" for k, v in params.items())
    url = f"{POLYGONSCAN_BASE}?{query}"
    for attempt in range(3):
        try:
            req = Request(url, headers={"User-Agent": "RN1-Analyzer/2.0"})
            resp = urlopen(req, timeout=30)
            data = json.loads(resp.read().decode())
            if data.get("status") == "0" and "rate limit" in data.get("message", "").lower():
                print(f"  Rate limited, waiting 6s...")
                time.sleep(6)
                continue
            time.sleep(0.3)
            return data
        except (URLError, HTTPError) as e:
            print(f"  API error (attempt {attempt+1}): {e}")
            time.sleep(3)
    return {"status": "0", "message": "Failed", "result": []}


def safe_int(val, default=0):
    try: return int(val)
    except: return default


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--api-key", required=True)
    parser.add_argument("--wallet", default=RN1_WALLET)
    parser.add_argument("--max-pages", type=int, default=20)
    args = parser.parse_args()
    
    api_key = args.api_key
    wallet = args.wallet.lower()
    
    print("=" * 60)
    print(f"RN1 On-Chain Analyzer v2")
    print(f"Wallet: {wallet}")
    print("=" * 60)
    
    results = {"wallet": wallet}
    
    # ═══ 1. WALLET TYPE ═══
    print("\n[1/6] Checking wallet type...")
    data = api_call({"module": "proxy", "action": "eth_getCode",
                     "address": wallet, "tag": "latest"}, api_key)
    code = data.get("result", "0x")
    is_contract = code != "0x" and len(code) > 2
    results["wallet_type"] = {
        "is_contract": is_contract,
        "type": "Smart Contract" if is_contract else "EOA",
        "code_length": len(code),
    }
    if is_contract:
        print(f"  SMART CONTRACT! Code: {len(code)} chars")
        print(f"  He deployed custom on-chain trading logic.")
    else:
        print(f"  EOA (regular wallet)")
    
    # ═══ 2. FIND THE DEPLOYER / OWNER EOA ═══
    # The contract was created by some EOA — that's his real wallet
    print("\n[2/6] Finding contract deployer...")
    
    # Method 1: Check creation tx
    data = api_call({"module": "contract", "action": "getcontractcreation",
                     "contractaddresses": wallet}, api_key)
    deployer = None
    creation_tx = None
    if data.get("status") == "1" and data.get("result"):
        info = data["result"][0] if isinstance(data["result"], list) else data["result"]
        deployer = info.get("contractCreator", "").lower()
        creation_tx = info.get("txHash", "")
        print(f"  Deployer EOA: {deployer}")
        print(f"  Creation tx: {creation_tx}")
    else:
        print(f"  Could not find deployer via API, trying txlist...")
        # Fallback: first transaction TO the contract
        data = api_call({"module": "account", "action": "txlist",
                         "address": wallet, "startblock": "0", "endblock": "99999999",
                         "page": "1", "offset": "5", "sort": "asc"}, api_key)
        txs = data.get("result", [])
        if isinstance(txs, list) and txs:
            deployer = txs[0].get("from", "").lower()
            creation_tx = txs[0].get("hash", "")
            print(f"  Likely deployer: {deployer}")
    
    results["deployer"] = {"address": deployer, "creation_tx": creation_tx}
    
    # ═══ 3. PULL INTERNAL TRANSACTIONS (contract receives these) ═══
    print("\n[3/6] Pulling internal transactions...")
    all_internal = []
    page = 1
    while page <= args.max_pages:
        print(f"  Page {page}...")
        data = api_call({"module": "account", "action": "txlistinternal",
                         "address": wallet, "startblock": "0", "endblock": "99999999",
                         "page": str(page), "offset": "10000", "sort": "asc"}, api_key)
        txs = data.get("result", [])
        if not txs or not isinstance(txs, list):
            break
        all_internal.extend(txs)
        print(f"    Got {len(txs)} (total: {len(all_internal)})")
        if len(txs) < 10000:
            break
        page += 1
    print(f"  Total internal txs: {len(all_internal)}")
    
    # Analyze internal txs
    if all_internal:
        callers = Counter()
        targets = Counter()
        timestamps = []
        values = []
        errors = 0
        
        for tx in all_internal:
            fr = tx.get("from", "").lower()
            to = tx.get("to", "").lower()
            callers[fr] += 1
            targets[to] += 1
            ts = safe_int(tx.get("timeStamp"))
            if ts > 0:
                timestamps.append(ts)
            val = safe_int(tx.get("value"))
            if val > 0:
                values.append(val / 1e18)  # MATIC
            if tx.get("isError") == "1":
                errors += 1
        
        # Timing from internal txs
        timing = {}
        if len(timestamps) > 10:
            ts_s = sorted(timestamps)
            gaps = [ts_s[i]-ts_s[i-1] for i in range(1, len(ts_s)) if 0 < ts_s[i]-ts_s[i-1] < 86400*7]
            if gaps:
                gs = sorted(gaps)
                timing = {
                    "first": datetime.utcfromtimestamp(ts_s[0]).isoformat(),
                    "last": datetime.utcfromtimestamp(ts_s[-1]).isoformat(),
                    "span_days": round((ts_s[-1] - ts_s[0]) / 86400),
                    "total_gaps": len(gs),
                    "median_gap_sec": gs[len(gs)//2],
                    "mean_gap_sec": round(sum(gs)/len(gs), 1),
                    "under_2s": round(len([g for g in gs if g <= 2])/len(gs)*100, 1),
                    "under_5s": round(len([g for g in gs if g <= 5])/len(gs)*100, 1),
                    "under_30s": round(len([g for g in gs if g <= 30])/len(gs)*100, 1),
                    "under_60s": round(len([g for g in gs if g <= 60])/len(gs)*100, 1),
                    "over_1hr": round(len([g for g in gs if g > 3600])/len(gs)*100, 1),
                }
        
        # Top callers (who triggers his contract?)
        caller_list = []
        for addr, cnt in callers.most_common(10):
            label = KNOWN_CONTRACTS.get(addr, "")
            if addr == wallet: label = "Self (re-entrant)"
            if addr == deployer: label = "OWNER EOA"
            caller_list.append({"address": addr, "label": label, "count": cnt,
                               "pct": round(cnt/len(all_internal)*100, 1)})
        
        # Top targets (what does his contract call?)
        target_list = []
        for addr, cnt in targets.most_common(10):
            label = KNOWN_CONTRACTS.get(addr, "")
            if addr == wallet: label = "Self"
            target_list.append({"address": addr, "label": label, "count": cnt,
                               "pct": round(cnt/len(all_internal)*100, 1)})
        
        results["internal_txs"] = {
            "total": len(all_internal),
            "errors": errors,
            "error_rate": round(errors/len(all_internal)*100, 2) if all_internal else 0,
            "callers": caller_list,
            "targets": target_list,
            "timing": timing,
            "value_transfers": len(values),
        }
    
    # ═══ 4. PULL DEPLOYER'S NORMAL TRANSACTIONS ═══
    # The deployer EOA is the one actually sending txs to the blockchain
    deployer_txs = []
    if deployer:
        print(f"\n[4/6] Pulling deployer EOA transactions ({deployer[:10]}...)...")
        page = 1
        while page <= args.max_pages:
            print(f"  Page {page}...")
            data = api_call({"module": "account", "action": "txlist",
                             "address": deployer, "startblock": "0", "endblock": "99999999",
                             "page": str(page), "offset": "10000", "sort": "asc"}, api_key)
            txs = data.get("result", [])
            if not txs or not isinstance(txs, list):
                break
            deployer_txs.extend(txs)
            print(f"    Got {len(txs)} (total: {len(deployer_txs)})")
            if len(txs) < 10000:
                break
            page += 1
        print(f"  Deployer total txs: {len(deployer_txs)}")
        
        if deployer_txs:
            # Where does the deployer send txs?
            dep_targets = Counter()
            dep_methods = Counter()
            dep_gas = []
            dep_timestamps = []
            dep_nonces = []
            dep_errors = 0
            
            for tx in deployer_txs:
                if tx.get("from", "").lower() == deployer:
                    to = tx.get("to", "").lower()
                    dep_targets[to] += 1
                    inp = tx.get("input", "0x")
                    if len(inp) >= 10:
                        dep_methods[inp[:10]] += 1
                    gp = safe_int(tx.get("gasPrice"))
                    if gp > 0:
                        dep_gas.append(gp / 1e9)
                    ts = safe_int(tx.get("timeStamp"))
                    if ts > 0:
                        dep_timestamps.append(ts)
                    dep_nonces.append(safe_int(tx.get("nonce")))
                    if tx.get("isError") == "1":
                        dep_errors += 1
            
            # Top targets
            dep_target_list = []
            for addr, cnt in dep_targets.most_common(10):
                label = KNOWN_CONTRACTS.get(addr, "")
                if addr == wallet: label = "HIS TRADING CONTRACT"
                dep_target_list.append({"address": addr, "label": label, "count": cnt})
            
            # Top methods
            dep_method_list = []
            for mid, cnt in dep_methods.most_common(10):
                label = KNOWN_METHODS.get(mid, "Unknown")
                dep_method_list.append({"method_id": mid, "label": label, "count": cnt})
            
            # Gas analysis
            gas_info = {}
            if dep_gas:
                gp = sorted(dep_gas)
                gas_info = {
                    "median_gwei": round(gp[len(gp)//2], 2),
                    "avg_gwei": round(sum(gp)/len(gp), 2),
                    "p10_gwei": round(gp[int(len(gp)*0.1)], 2),
                    "p90_gwei": round(gp[int(len(gp)*0.9)], 2),
                    "min_gwei": round(min(gp), 2),
                    "max_gwei": round(max(gp), 2),
                    "fixed_gas": gp[int(len(gp)*0.25)] == gp[int(len(gp)*0.75)],
                    "unique_values": len(set(round(g, 1) for g in gp)),
                }
            
            # Timing
            dep_timing = {}
            if len(dep_timestamps) > 10:
                ts_s = sorted(dep_timestamps)
                gaps = [ts_s[i]-ts_s[i-1] for i in range(1, len(ts_s)) if 0 < ts_s[i]-ts_s[i-1] < 86400*7]
                if gaps:
                    gs = sorted(gaps)
                    dep_timing = {
                        "first": datetime.utcfromtimestamp(ts_s[0]).isoformat(),
                        "last": datetime.utcfromtimestamp(ts_s[-1]).isoformat(),
                        "span_days": round((ts_s[-1] - ts_s[0]) / 86400),
                        "median_gap_sec": gs[len(gs)//2],
                        "mean_gap_sec": round(sum(gs)/len(gs), 1),
                        "under_2s": round(len([g for g in gs if g <= 2])/len(gs)*100, 1),
                        "under_5s": round(len([g for g in gs if g <= 5])/len(gs)*100, 1),
                        "under_30s": round(len([g for g in gs if g <= 30])/len(gs)*100, 1),
                        "under_60s": round(len([g for g in gs if g <= 60])/len(gs)*100, 1),
                        "over_1hr": round(len([g for g in gs if g > 3600])/len(gs)*100, 1),
                    }
            
            # Nonce analysis
            nonce_info = {}
            if dep_nonces:
                ns = sorted(set(dep_nonces))
                max_n = max(ns)
                min_n = min(ns)
                expected = set(range(min_n, max_n + 1))
                missing = expected - set(ns)
                nonce_info = {
                    "min": min_n, "max": max_n,
                    "unique": len(ns),
                    "gaps": len(missing),
                    "gap_pct": round(len(missing)/(max_n-min_n+1)*100, 2) if max_n > min_n else 0,
                }
            
            # Hourly
            dep_hours = Counter(datetime.utcfromtimestamp(ts).hour for ts in dep_timestamps)
            top3_hours = dep_hours.most_common(3)
            
            results["deployer_analysis"] = {
                "address": deployer,
                "total_txs": len(deployer_txs),
                "outgoing": sum(1 for tx in deployer_txs if tx.get("from","").lower() == deployer),
                "errors": dep_errors,
                "error_rate": round(dep_errors/len(deployer_txs)*100, 2) if deployer_txs else 0,
                "targets": dep_target_list,
                "methods": dep_method_list,
                "gas": gas_info,
                "timing": dep_timing,
                "nonces": nonce_info,
                "peak_hours_utc": {str(h): c for h, c in top3_hours},
            }
    
    # ═══ 5. ERC20 TRANSFERS ═══
    print("\n[5/6] Checking ERC20 transfers...")
    data = api_call({"module": "account", "action": "tokentx",
                     "address": wallet, "startblock": "0", "endblock": "99999999",
                     "page": "1", "offset": "200", "sort": "desc"}, api_key)
    txs = data.get("result", [])
    if isinstance(txs, list) and txs:
        tokens = Counter(tx.get("tokenSymbol", "?") for tx in txs)
        results["erc20"] = {"count": len(txs), "tokens": dict(tokens.most_common(10))}
        print(f"  ERC20 transfers: {len(txs)}")
    else:
        results["erc20"] = {"count": 0}
    
    # ═══ 6. ERC1155 TRANSFERS ═══
    print("\n[6/6] Checking ERC1155 transfers...")
    data = api_call({"module": "account", "action": "token1155tx",
                     "address": wallet, "startblock": "0", "endblock": "99999999",
                     "page": "1", "offset": "200", "sort": "desc"}, api_key)
    txs = data.get("result", [])
    if isinstance(txs, list) and txs:
        results["erc1155"] = {"count": len(txs)}
        print(f"  ERC1155 transfers: {len(txs)}")
    else:
        results["erc1155"] = {"count": 0}
    
    # ═══ BOT DETECTION ═══
    bot_signals = []
    gas = results.get("deployer_analysis", {}).get("gas", {})
    if gas.get("fixed_gas"):
        bot_signals.append("Fixed gas price (automated)")
    if gas.get("unique_values", 999) < 5:
        bot_signals.append(f"Only {gas['unique_values']} unique gas values (hardcoded)")
    
    dep_timing = results.get("deployer_analysis", {}).get("timing", {})
    if dep_timing.get("under_5s", 0) > 30:
        bot_signals.append(f"{dep_timing['under_5s']}% of txs under 5s apart (high-speed bot)")
    
    nonces = results.get("deployer_analysis", {}).get("nonces", {})
    if nonces.get("gaps", 999) == 0:
        bot_signals.append("Zero nonce gaps (perfect sequential execution)")
    
    dep_err = results.get("deployer_analysis", {}).get("error_rate", 100)
    if dep_err < 2:
        bot_signals.append(f"Low error rate: {dep_err}% (precise execution)")
    
    results["wallet_type"]["is_contract"] = is_contract
    if is_contract:
        bot_signals.append("SMART CONTRACT wallet (custom on-chain logic)")
    
    results["bot_signals"] = bot_signals
    
    # ═══ SUMMARY ═══
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    
    print(f"\n  Wallet type: {results['wallet_type']['type']}")
    if deployer:
        print(f"  Owner EOA: {deployer}")
    
    itx = results.get("internal_txs", {})
    print(f"  Internal txs: {itx.get('total', 0)}")
    
    da = results.get("deployer_analysis", {})
    if da:
        print(f"  Deployer txs: {da.get('total_txs', 0)}")
        print(f"  Error rate: {da.get('error_rate', '?')}%")
        
        tgts = da.get("targets", [])
        if tgts:
            print(f"\n  Deployer sends txs to:")
            for t in tgts[:5]:
                label = t['label'] or t['address'][:20]
                print(f"    {label:40s} {t['count']:>6}")
        
        meths = da.get("methods", [])
        if meths:
            print(f"\n  Methods called:")
            for m in meths[:5]:
                print(f"    {m['label']:40s} {m['count']:>6}")
        
        g = da.get("gas", {})
        if g:
            print(f"\n  Gas: median={g.get('median_gwei','?')} gwei, "
                  f"fixed={g.get('fixed_gas','?')}, "
                  f"unique={g.get('unique_values','?')}")
        
        t = da.get("timing", {})
        if t:
            print(f"  Timing: median gap={t.get('median_gap_sec','?')}s, "
                  f"<5s={t.get('under_5s','?')}%, "
                  f"<60s={t.get('under_60s','?')}%")
    
    if bot_signals:
        print(f"\n  Bot signals:")
        for b in bot_signals:
            print(f"    > {b}")
    
    output = "rn1_onchain_analysis.json"
    with open(output, "w") as f:
        json.dump(results, f, indent=2, default=str)
    
    print(f"\n{'=' * 60}")
    print(f"Saved: {output} ({os.path.getsize(output)/1024:.1f} KB)")
    print(f"Upload this file to Claude!")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
