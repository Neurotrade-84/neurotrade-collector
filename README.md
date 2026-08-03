# NeuroTrade Order Book Collector (Binance)

A minimal, standalone piece pulled out of the main NeuroTrade project - just what's
needed to continuously collect Binance order book depth + trade flow for BTC, ETH,
BNB, and SOL. No API keys, no secrets, no other project files required.

## Structure
```
neurotrade-collector/
  app/
    collect_orderbook.py
  requirements.txt
  neurotrade-collector.service
  README.md
```
(Keep `collect_orderbook.py` inside `app/` - it computes its output folder as
"one level above its own location", so this layout matters.)

## Quick manual run (test before setting up the always-on service)
```bash
sudo apt update && sudo apt install -y python3 python3-pip
pip3 install -r requirements.txt
python3 app/collect_orderbook.py --provider binance --assets BTCUSDT,ETHUSDT,BNBUSDT,SOLUSDT --interval 60
```
Let it run a minute or two, confirm you see lines like:
```
[...] BTCUSDT: mid=... book_imbalance=... trade_imbalance=... (N trades) (N snapshots so far)
```
Then Ctrl+C and move on to the always-on setup below.

## Always-on setup (systemd - survives reboots and crashes)
See the detailed steps inside `neurotrade-collector.service`. Short version:
```bash
sudo cp neurotrade-collector.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now neurotrade-collector
journalctl -u neurotrade-collector -f   # watch it live
```

## Output
CSV files, one per asset per data type, appended to forever:
```
data_cache/orderbook/binance/BTCUSDT_orderbook.csv
data_cache/orderbook/binance/BTCUSDT_trades.csv
data_cache/orderbook/binance/ETHUSDT_orderbook.csv
... etc for ETHUSDT, BNBUSDT, SOLUSDT
```

## Getting the data back out later
Whenever you want to pull the collected CSVs back to your main machine:
```bash
scp -r user@your-vps-ip:/opt/neurotrade-collector/data_cache ./data_cache
```

## Why this is a separate repo, not the whole NeuroTrade project
`collect_orderbook.py` has zero imports from the rest of NeuroTrade (only Python's
standard library + `requests`) - it doesn't need the belief engine, backtester, or
anything else. Keeping this repo small means less to install, less that could break
on a fresh VPS, and no risk of accidentally exposing anything else from the main
project if this repo is public.

