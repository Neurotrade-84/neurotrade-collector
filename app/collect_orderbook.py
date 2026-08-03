"""
Collects order book AND trade-flow snapshots over time, from Nobitex and/or Binance.
Order-flow data cannot be backfilled retroactively (unlike OHLCV) - exchanges only
expose the CURRENT book/recent trades, not history - so this needs to run continuously
starting now.

Data streams per asset, written to separate CSVs:

1. ORDER BOOK (top-of-book + near-price depth), from both:
   - Nobitex: GET https://apiv2.nobitex.ir/v2/orderbook/<SYMBOL>
   - Binance: GET https://api.binance.com/api/v3/depth?symbol=<SYMBOL>&limit=100

2. TRADE FLOW (aggressor side of each recent trade - who "crossed the spread"),
   only from Binance (Nobitex trade-flow has been removed):
   - Binance: GET https://api.binance.com/api/v3/trades?symbol=<SYMBOL>&limit=500
     -> isBuyerMaker=true means the BUYER was passive (i.e. a SELL-side aggressor) -
     inverted so aggressor_side is "buy"/"sell".

Each poll reduces the trade list to one summary row (buy/sell volume, count, VWAP,
signed imbalance) rather than storing every individual trade - keeps file size sane
over months while preserving the signal (net order flow) this is actually for.

Usage:
    python app/collect_orderbook.py --provider nobitex --assets BTCUSDT,ETHUSDT --interval 60
    python app/collect_orderbook.py --provider binance --assets BTCUSDT,ETHUSDT --interval 60

Note on Nobitex from a non-Iranian IP (e.g. a Frankfurt VPS): Nobitex is an
Iran-focused exchange operating under sanctions constraints - it is NOT confirmed
whether its public API behaves identically from foreign IPs (could be fine, could be
geo-restricted, could behave differently than from Iran). Test with a short
`--interval 10` run from the VPS before committing to a long collection run there.
Binance's public market-data endpoints (depth/trades) are unauthenticated and
generally reliable from EU IPs, including Frankfurt.
"""
import sys
import os
import csv
import time
import argparse
from pathlib import Path
from datetime import datetime, timezone
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import requests

OUTPUT_DIR = Path(__file__).resolve().parents[1] / "data_cache" / "orderbook"
NEAR_PCT = 0.005
REQUEST_TIMEOUT_S = 10
MAX_RETRIES = 3


def _get_with_retry(method, url, **kwargs):
    last_err = None
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.request(method, url, timeout=REQUEST_TIMEOUT_S, **kwargs)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            last_err = e
            time.sleep(2 * (attempt + 1))
    raise RuntimeError(f"Failed after {MAX_RETRIES} tries: {last_err}")


# ---------- order book ----------

def fetch_orderbook(provider: str, symbol: str) -> dict:
    """Returns a normalized {"bids": [(price, amount), ...], "asks": [...]} regardless
    of exchange, so summarize_orderbook() doesn't need to know which exchange it came
    from."""
    if provider == "nobitex":
        # uses apiv2.nobitex.ir (not api.nobitex.ir) - matches
        # data_engine/providers/nobitex_provider.py's BASE_URL; api.nobitex.ir failed
        # DNS resolution on at least one user's network (2026-07-31).
        data = _get_with_retry("GET", f"https://apiv2.nobitex.ir/v2/orderbook/{symbol}")
        if data.get("status") != "ok":
            raise ValueError(f"Nobitex orderbook status={data.get('status')}")
        bids = [(float(p), float(a)) for p, a in data.get("bids", [])]
        asks = [(float(p), float(a)) for p, a in data.get("asks", [])]
        last_trade_price = data.get("lastTradePrice")
    elif provider == "binance":
        data = _get_with_retry("GET", "https://api.binance.com/api/v3/depth",
                                params={"symbol": symbol, "limit": 100})
        bids = [(float(p), float(a)) for p, a in data.get("bids", [])]
        asks = [(float(p), float(a)) for p, a in data.get("asks", [])]
        last_trade_price = None
    else:
        raise ValueError(f"Unknown provider: {provider}")
    return {"bids": bids, "asks": asks, "last_trade_price": last_trade_price}


def summarize_orderbook(ob: dict) -> dict:
    bids, asks = ob["bids"], ob["asks"]
    if not bids or not asks:
        raise ValueError("Empty bids or asks - skipping this snapshot")

    best_bid = max(p for p, a in bids)
    best_ask = min(p for p, a in asks)
    mid = (best_bid + best_ask) / 2
    spread_pct = (best_ask - best_bid) / mid if mid else float("nan")

    bid_l1_amount = max(bids, key=lambda x: x[0])[1]
    ask_l1_amount = min(asks, key=lambda x: x[0])[1]

    near_lo, near_hi = mid * (1 - NEAR_PCT), mid * (1 + NEAR_PCT)
    bid_vol_near = sum(a for p, a in bids if p >= near_lo)
    ask_vol_near = sum(a for p, a in asks if p <= near_hi)
    total = bid_vol_near + ask_vol_near
    imbalance = (bid_vol_near - ask_vol_near) / total if total > 0 else 0.0

    return {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "best_bid": best_bid,
        "best_ask": best_ask,
        "mid_price": round(mid, 8),
        "spread_pct": round(spread_pct, 6),
        "bid_volume_l1": round(bid_l1_amount, 6),
        "ask_volume_l1": round(ask_l1_amount, 6),
        "bid_volume_near": round(bid_vol_near, 6),
        "ask_volume_near": round(ask_vol_near, 6),
        "imbalance": round(imbalance, 6),
        "last_trade_price": ob.get("last_trade_price"),
    }


# ---------- trade flow (Binance only) ----------

def fetch_trades(provider: str, symbol: str) -> list:
    """Returns a normalized list of {price, amount, aggressor_side, timestamp} -
    aggressor_side is "buy" if the trade was initiated by a market buy (crossed the
    ask), "sell" if initiated by a market sell (crossed the bid).
    Only supported for Binance (Nobitex trade-flow removed)."""
    if provider == "binance":
        data = _get_with_retry("GET", "https://api.binance.com/api/v3/trades",
                                params={"symbol": symbol, "limit": 500})
        return [{
            "price": float(t["price"]),
            "amount": float(t["qty"]),
            # Binance's isBuyerMaker=True means the buyer was passive -> the trade was
            # SELL-initiated. Inverted here so aggressor_side is "buy"/"sell".
            "aggressor_side": "sell" if t["isBuyerMaker"] else "buy",
            "timestamp": t["time"],
        } for t in data]
    else:
        raise ValueError(f"Trade flow is only supported for binance (not {provider})")


def summarize_trades(trades: list) -> dict:
    if not trades:
        return {
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "trade_count": 0, "buy_volume": 0.0, "sell_volume": 0.0,
            "trade_imbalance": 0.0, "vwap": None,
        }
    buy_vol = sum(t["amount"] for t in trades if t["aggressor_side"] == "buy")
    sell_vol = sum(t["amount"] for t in trades if t["aggressor_side"] == "sell")
    total_vol = buy_vol + sell_vol
    vwap = sum(t["price"] * t["amount"] for t in trades) / total_vol if total_vol > 0 else None
    imbalance = (buy_vol - sell_vol) / total_vol if total_vol > 0 else 0.0

    return {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "trade_count": len(trades),
        "buy_volume": round(buy_vol, 6),
        "sell_volume": round(sell_vol, 6),
        "trade_imbalance": round(imbalance, 6),
        "vwap": round(vwap, 8) if vwap is not None else None,
        "last_price": trades[-1]["price"],
    }


# ---------- storage ----------

def append_row(csv_path: Path, row: dict):
    is_new = not csv_path.exists()
    with open(csv_path, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(row.keys()))
        if is_new:
            writer.writeheader()
        writer.writerow(row)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--provider", default="binance", choices=["nobitex", "binance"])
    parser.add_argument("--assets", default="BTCUSDT,ETHUSDT,BNBUSDT,SOLUSDT")
    parser.add_argument("--interval", type=int, default=60,
                         help="seconds between polls, per asset (default 60)")
    args = parser.parse_args()
    assets = [a.strip() for a in args.assets.split(",")]

    provider_dir = OUTPUT_DIR / args.provider
    provider_dir.mkdir(parents=True, exist_ok=True)

    if args.provider == "nobitex":
        print(f"Collecting order book only from {args.provider} for {assets}, "
              f"every {args.interval}s.")
    else:
        print(f"Collecting order book + trade flow from {args.provider} for {assets}, "
              f"every {args.interval}s.")
    print(f"Writing to: {provider_dir}")
    print("Leave this running (Ctrl+C to stop safely - already-written rows are kept).\n")

    poll_count = 0
    while True:
        for asset in assets:
            try:
                ob = fetch_orderbook(args.provider, asset)
                ob_row = summarize_orderbook(ob)
                append_row(provider_dir / f"{asset}_orderbook.csv", ob_row)

                if args.provider == "binance":
                    trades = fetch_trades(args.provider, asset)
                    tr_row = summarize_trades(trades)
                    append_row(provider_dir / f"{asset}_trades.csv", tr_row)

                    poll_count += 1
                    print(f"[{ob_row['timestamp_utc']}] {asset}: mid={ob_row['mid_price']} "
                          f"book_imbalance={ob_row['imbalance']:+.3f} "
                          f"trade_imbalance={tr_row['trade_imbalance']:+.3f} "
                          f"({tr_row['trade_count']} trades) ({poll_count} snapshots so far)")
                else:
                    poll_count += 1
                    print(f"[{ob_row['timestamp_utc']}] {asset}: mid={ob_row['mid_price']} "
                          f"book_imbalance={ob_row['imbalance']:+.3f} "
                          f"({poll_count} snapshots so far)")
            except Exception as e:
                print(f"[warning] {asset}: {e} - will retry next cycle")
        time.sleep(args.interval)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nStopped. Existing CSV files are intact - just re-run the same command "
              "to resume collecting.")
