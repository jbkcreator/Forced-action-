#!/usr/bin/env bash
# Run the Pinellas case-number backfill as N parallel streams on a Linux server.
#
# PRE-REQ (see runbook): one CF profile must be WARMED ON THIS SERVER (its IP),
# then copied to data/cf_session/edge_profile_pin0 .. edge_profile_pin<N-1>.
# cf_clearance is IP-bound — a profile warmed elsewhere will NOT pass here.
#
# Usage (inside `screen`/`tmux` so it survives disconnect):
#     screen -S backfill
#     bash scripts/run_backfill_parallel.sh 8
#     # detach: Ctrl-A D     reattach: screen -r backfill
set -euo pipefail

N="${1:-6}"                       # number of parallel streams (recommend 4–8)
PROFILE_PREFIX="${PROFILE_PREFIX:-pin}"
LOGDIR="${LOGDIR:-scratch/backfill_logs}"
mkdir -p "$LOGDIR"

# One shared virtual display for all headed Edge instances (CF needs headed).
if [ -z "${DISPLAY:-}" ]; then
  Xvfb :99 -screen 0 1400x900x24 >/dev/null 2>&1 &
  export DISPLAY=:99
  sleep 1
fi
echo "DISPLAY=$DISPLAY  streams=$N"

# Sanity: every profile dir must exist (warmed+copied beforehand).
for i in $(seq 0 $((N-1))); do
  d="data/cf_session/edge_profile_${PROFILE_PREFIX}${i}"
  [ -d "$d" ] || { echo "MISSING profile dir: $d  (warm one, then copy to ${PROFILE_PREFIX}0..${PROFILE_PREFIX}$((N-1)))"; exit 1; }
done

pids=()
for i in $(seq 0 $((N-1))); do
  PYTHONIOENCODING=utf-8 python scripts/backfill_pinellas_case_numbers.py \
      --profile "${PROFILE_PREFIX}${i}" --shard "${i}/${N}" \
      > "${LOGDIR}/stream_${i}.log" 2>&1 &
  pids+=($!)
  echo "launched stream ${i}/${N} -> ${LOGDIR}/stream_${i}.log (pid $!)"
  sleep 5          # stagger starts so N sessions don't hit CF in the same instant
done

echo "all ${N} streams running. tail -f ${LOGDIR}/stream_*.log to watch."
wait "${pids[@]}"
echo "ALL STREAMS DONE."
