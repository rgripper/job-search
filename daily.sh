#!/usr/bin/env bash
# Daily job search. Edit the flags below to taste, then run ./daily.sh
# (or add to cron:  0 8 * * *  cd /home/vladimir/github/job-search && ./daily.sh >> daily.log 2>&1)
set -euo pipefail
cd "$(dirname "$0")"

uv run job_search.py \
  --profiles engineer product \
  --city Berlin \
  --country DE \
  --max-age-days 7 \
  --require-date \
  --open

# --require-date = only roles with a real posted_at within 7 days (small, sharp list).
# Drop it for a much larger list that also keeps roles with no posting date.

echo "Open results/latest.html"
