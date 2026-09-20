"""
Real-time PAPER-TRADING watcher for the trade_to_quote signal.

This is NOT a live-money trading bot. It reads the same CSV files that
collect_orderbook.py is already writing (it does not place any real orders,
does not need API keys, does not touch the exchange at all) and simulates
LONG-ONLY entries using thresholds that were fixed from OLD data - it never
re-fits thresholds on new data, to keep this a genuine forward test.

Why long-only: the Sep 2026 out-of-sample check found the short side failed
(hit rate 38.5%, avg -0.21%) while longs held up (hit rate 60.0%, avg +0.62%)
on a small sample (n=26 short / n=55 long). Short is disabled here until a
larger out-of-sample sample says otherwise - re-enable only after re-testing.

Trend filter (added 2026-09-20): a winner/loser diagnostic on the validated
trade_to_quote trades found trade_imbalance_roll_5m (sustained 5-snapshot buy
pressure, not just the instantaneous trigger) differed meaningfully between
winners and losers. Tested as an actual forward filter (not just a retrospective
comparison - see the spread_pct trap below) on the Sep 8-18 out-of-sample window:
avg_net went from -0.0168% to +0.0263%, hit rate from 44.3% to 49.4% (n dropped
from 131 to 85). Promising, but still a small sample - keep watching whether this
holds as more paper-trading data accumulates, same as the base signal itself.

NOT included: a retrospective spread_pct comparison looked equally strong
(winners' spread 6x tighter) but made results WORSE when tested as an actual
filter - a textbook retrospective-correlation-vs-predictive-filter trap. Don't
add a spread filter here without re-running this same forward-test discipline.

Rule (frozen, do not tune on live data - only re-derive from a NEW completed
calibration window and document the change):
    signal   = LONG when trade_to_quote >= HI_THRESHOLD
               AND trade_imbalance_roll_5m > TREND_THRESHOLD
    hold     = 15 snapshots (~15 minutes at the current ~62s poll interval)
    cost     = configurable, default assumes 0.10%% round trip (Binance
               Regular User taker rate - the realistic default until a maker
               account/tier is confirmed)

Logs every simulated trade (entry time/price, exit time/price, net return) to
paper_trades_<asset>.csv, appending forever - safe to stop/restart, it resumes
from where it left off using the last logged timestamp.

Config file (recommended, added 2026-09-20): instead of hardcoding thresholds in
the systemd ExecStart line (which meant regenerating the whole unit file for
every experiment), pass --config pointing at a small JSON file. CLI args still
work and override the config file, for one-off manual runs.

    config.json:
        {"hi_threshold": 0.000102, "trend_threshold": 0.012156,
         "hold_snapshots": 15, "cost": 0.0010, "poll_seconds": 30}

    python3 paper_trade_watcher.py --orderbook ... --trades ... --out ... \
        --config config.json

Usage (run per asset, ideally under systemd like the collector itself):
    python3 paper_trade_watcher.py \
        --orderbook /opt/neurotrade-collector/data_cache/orderbook/binance/BTCUSDT_orderbook.csv \
        --trades    /opt/neurotrade-collector/data_cache/orderbook/binance/BTCUSDT_trades.csv \
        --out paper_trades_BTCUSDT.csv \
        --config /opt/neurotrade-collector/paper_trader_config.json
"""
import argparse
import csv
import json
import time
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd


def load_new_rows(ob_path, tr_path, last_seen_ts):
    ob = pd.read_csv(ob_path)
    tr = pd.read_csv(tr_path)
    ob["timestamp_utc"] = pd.to_datetime(ob["timestamp_utc"], utc=True)
    tr["timestamp_utc"] = pd.to_datetime(tr["timestamp_utc"], utc=True)

    # Merge by TIMESTAMP (nearest match within a tight tolerance), not by row
    # position. The two CSVs are written by two separate append_row() calls in
    # collect_orderbook.py - if either write is ever delayed or fails on one
    # side (a transient network hiccup, a race between this watcher reading
    # mid-write), the two files can end up with different row counts. Concat by
    # position then silently misaligns EVERY row after that point forever,
    # pairing each mid_price with the wrong last_trade_price - this was
    # confirmed as the actual cause of the live/backtest discrepancy (44,395
    # orderbook rows vs 44,396 trades rows on 2026-09-18).
    ob_sorted = ob.sort_values("timestamp_utc")
    tr_sorted = tr[["timestamp_utc", "last_price", "trade_imbalance"]].sort_values("timestamp_utc")
    df = pd.merge_asof(
        ob_sorted[["timestamp_utc", "mid_price", "last_trade_price"]],
        tr_sorted,
        on="timestamp_utc",
        direction="nearest",
        tolerance=pd.Timedelta(seconds=5),
    )
    df["last_trade_price"] = df["last_trade_price"].fillna(df["last_price"])
    df["trade_to_quote"] = (df["last_trade_price"] - df["mid_price"]) / df["mid_price"]
    # Rolling mean computed on the FULL merged history (not just the new-rows
    # slice below) so the window has proper context right after a restart,
    # instead of being empty/short for the first few polls.
    df["trade_imbalance_roll_5m"] = df["trade_imbalance"].rolling(5).mean()

    if last_seen_ts is not None:
        df = df[df["timestamp_utc"] > last_seen_ts]
    return df.reset_index(drop=True)


def append_trade_log(out_path: Path, row: dict):
    is_new = not out_path.exists()
    with open(out_path, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(row.keys()))
        if is_new:
            writer.writeheader()
        writer.writerow(row)


def get_last_logged_timestamp(out_path: Path):
    if not out_path.exists():
        return None
    try:
        df = pd.read_csv(out_path)
        if len(df) == 0:
            return None
        return pd.to_datetime(df["entry_time"].max(), utc=True)
    except Exception:
        return None


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--orderbook", required=True)
    p.add_argument("--trades", required=True)
    p.add_argument("--out", required=True)
    p.add_argument("--config", default=None,
                    help="path to a JSON file with hi_threshold/trend_threshold/hold_snapshots/"
                         "cost/poll_seconds - lets you change thresholds by editing one small file "
                         "and restarting, instead of regenerating the whole systemd unit each time. "
                         "CLI flags below override whatever the config file says.")
    p.add_argument("--hi-threshold", type=float, default=None,
                    help="fixed long-entry threshold for trade_to_quote, derived from an OLD calibration window - do not refit live")
    p.add_argument("--trend-threshold", type=float, default=None,
                    help="fixed minimum for trade_imbalance_roll_5m (sustained buy pressure) - "
                         "second, independently-required condition alongside hi-threshold")
    p.add_argument("--hold-snapshots", type=int, default=None)
    p.add_argument("--cost", type=float, default=None,
                    help="round-trip cost as a fraction, default 0.10%% (Binance Regular User taker)")
    p.add_argument("--poll-seconds", type=int, default=None)
    args = p.parse_args()

    # Config file provides defaults; any CLI flag actually passed overrides it.
    # Hardcoded fallbacks match the values validated so far, so a bare run with
    # neither --config nor the individual flags still does something sane.
    cfg = {"hi_threshold": 0.000102, "trend_threshold": 0.012156,
           "hold_snapshots": 15, "cost": 0.0010, "poll_seconds": 30}
    if args.config:
        cfg.update(json.loads(Path(args.config).read_text()))
    for key in ["hi_threshold", "trend_threshold", "hold_snapshots", "cost", "poll_seconds"]:
        cli_val = getattr(args, key)
        if cli_val is not None:
            cfg[key] = cli_val

    out_path = Path(args.out)
    last_ts = get_last_logged_timestamp(out_path)
    print(f"Resuming paper trading watcher. Last logged entry: {last_ts}")
    print(f"Rule: LONG when trade_to_quote >= {cfg['hi_threshold']} "
          f"AND trade_imbalance_roll_5m > {cfg['trend_threshold']}, "
          f"hold {cfg['hold_snapshots']} snapshots, cost {cfg['cost']:.3%}")

    open_position = None  # {"entry_time","entry_price","entry_idx_marker"}
    pending_close_after = None

    # Track how many NEW snapshots we've seen since a position was opened, since
    # this script polls a growing file rather than a live tick stream.
    seen_since_open = 0

    while True:
        try:
            new_rows = load_new_rows(args.orderbook, args.trades, last_ts)
        except Exception as e:
            print(f"[warning] failed to read CSVs: {e} - retrying next cycle")
            time.sleep(cfg["poll_seconds"])
            continue

        for _, row in new_rows.iterrows():
            last_ts = row["timestamp_utc"]

            if open_position is None:
                if row["trade_to_quote"] >= cfg["hi_threshold"] and \
                   pd.notna(row["trade_imbalance_roll_5m"]) and \
                   row["trade_imbalance_roll_5m"] > cfg["trend_threshold"]:
                    open_position = {
                        "entry_time": row["timestamp_utc"],
                        "entry_price": row["mid_price"],
                    }
                    seen_since_open = 0
                    print(f"[{row['timestamp_utc']}] OPEN long @ {row['mid_price']} "
                          f"(trade_to_quote={row['trade_to_quote']:.6f}, "
                          f"trend_5m={row['trade_imbalance_roll_5m']:.4f})")
            else:
                seen_since_open += 1
                if seen_since_open >= cfg["hold_snapshots"]:
                    exit_price = row["mid_price"]
                    raw_ret = exit_price / open_position["entry_price"] - 1
                    net_ret = raw_ret - cfg["cost"]
                    log_row = {
                        "entry_time": open_position["entry_time"],
                        "entry_price": open_position["entry_price"],
                        "exit_time": row["timestamp_utc"],
                        "exit_price": exit_price,
                        "raw_return": round(raw_ret, 6),
                        "net_return": round(net_ret, 6),
                    }
                    append_trade_log(out_path, log_row)
                    print(f"[{row['timestamp_utc']}] CLOSE @ {exit_price} "
                          f"net_return={net_ret:.4%}")
                    open_position = None
                    seen_since_open = 0

        time.sleep(cfg["poll_seconds"])


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nStopped. paper_trades CSV is intact - rerun the same command to resume "
              "(it picks up from the last logged entry_time automatically).")
