#!/bin/bash
# One-shot deploy script for neurotrade-paper-trader.
# Replaces the ~7 manual steps (stop, pull, verify, archive old results, reseed,
# restart, verify) with a single command. Safe to run any time the watcher's
# code or systemd file changes - it archives the previous results automatically
# so nothing is silently lost or mixed with a new rule's output.
#
# Usage: sudo bash deploy_paper_trader.sh
set -e  # stop immediately on any error - never leave things half-done

REPO_DIR="/opt/neurotrade-collector"
SERVICE_NAME="neurotrade-paper-trader"
OUT_CSV="$REPO_DIR/data_cache/paper_trades_BTCUSDT.csv"

echo "== 1/6: در حال توقف سرویس =="
systemctl stop "$SERVICE_NAME" || true

echo "== 2/6: در حال کشیدن کد جدید از GitHub =="
cd "$REPO_DIR"
git pull origin main

echo "== 3/6: تأیید نسخه‌ی فایل =="
if grep -q "trend-threshold" app/paper_trade_watcher.py; then
    echo "  فیلتر روند 5 دقیقه‌ای موجود است."
else
    echo "  هشدار: فیلتر روند در فایل دیده نشد - ممکن است کد قدیمی باشد."
fi

echo "== 4/6: آرشیو نتایج قبلی (اگر وجود داشته باشد) =="
if [ -f "$OUT_CSV" ]; then
    ARCHIVE_NAME="${OUT_CSV%.csv}_archived_$(date +%Y%m%d_%H%M%S).csv"
    mv "$OUT_CSV" "$ARCHIVE_NAME"
    echo "  آرشیو شد در: $ARCHIVE_NAME"
else
    echo "  فایلی برای آرشیو کردن وجود نداشت."
fi

echo "== 5/6: ساخت فایل بذر تازه =="
python3 -c "
import csv
from datetime import datetime, timezone
with open('$OUT_CSV', 'w', newline='') as f:
    w = csv.writer(f)
    w.writerow(['entry_time','entry_price','exit_time','exit_price','raw_return','net_return'])
    w.writerow([datetime.now(timezone.utc).isoformat(), 0, '', 0, 0, 0])
"

echo "== 6/6: استارت مجدد سرویس =="
systemctl daemon-reload
systemctl start "$SERVICE_NAME"
sleep 3
systemctl status "$SERVICE_NAME" --no-pager

echo ""
echo "تمام شد. برای دیدن لاگ زنده:"
echo "  journalctl -u $SERVICE_NAME -f"
