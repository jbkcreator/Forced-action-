# Server Runbook — Pinellas Case-Number Backfill (for Claude Code)

**Audience:** Claude Code running on the Linux server.
**Mission:** Backfill real court case numbers for Pinellas rows that lack one, by
reading the `CASENUMBER` field from the Official Records "Details" popup
(`officialrecords.mypinellasclerk.gov`), running N parallel streams to finish in
~1 hour. **No OCR, no 2captcha, no Anthropic key needed** — only DB access + a
Cloudflare-warmed Edge profile.

Work the steps in order. Each step has a **GATE** — do not proceed past a gate
until its check passes. Stop and report if a gate fails twice.

---

## 0. Context you need

- Branch: `feature/ocr-v2-pinellas-fixes` (already pushed).
- The job script: `scripts/backfill_pinellas_case_numbers.py`
  - flags: `--profile <name>` (CF Edge profile), `--shard i/n` (process every
    n-th candidate — disjoint streams), `--limit N` (testing), `--headless`.
  - Per row it opens the Official Records search → Details popup → reads
    `CASENUMBER`, converts to a dashed base UCN (e.g. `26001007ES` → `26-001007-ES`).
  - Writes:
    - `legal_proceedings` (Probate/Divorce): `case_number` currently holds the
      10-digit ORI instrument → overwrite with the court UCN; original instrument
      saved to `meta_data.ori_instrument_number`.
    - `legal_and_liens` (Judgment): fills the NULL `case_number`; `instrument_number` untouched.
  - **Idempotent + resumable:** every processed row gets
    `meta_data.casenumber_lookup ∈ {found, duplicate, none}`. Reruns skip marked
    rows. Transient failures stay unmarked and are retried on the next run.
  - **Collision-safe:** `legal_proceedings.case_number` is globally UNIQUE and
    many instruments map to one estate case → such rows are flagged
    `duplicate` (case_number left as-is), never crash.
- The launcher: `scripts/run_backfill_parallel.sh N` — fans out N sharded
  streams, one shared Xvfb, profiles `data/cf_session/edge_profile_pin0 .. pin{N-1}`.

**Per-row time is portal-bound (~16s, irreducible).** Speed comes only from
parallel streams (each profile = independent CF session).

---

## 1. Environment setup

```bash
cd <repo>
git pull origin feature/ocr-v2-pinellas-fixes
pip install -r requirements.txt
pip install nodriver 2captcha-python        # nodriver only for warming
sudo apt-get install -y microsoft-edge-stable xvfb   # REAL Edge is mandatory for CF
```

Create `.env` (it is gitignored — never committed). Minimum for this job:
```
DATABASE_URL=postgresql://<user>:<pass>@<host>:5432/<db>
```

Edge is auto-detected at `/usr/bin/microsoft-edge-stable`. If it lives elsewhere:
```bash
export CF_BYPASS_BROWSER_PATH=/path/to/microsoft-edge   # for the runtime
export CHROME_PATH=/path/to/microsoft-edge              # for the warming script
```

**GATE 1 — DB + Edge reachable:**
```bash
python -c "from src.core.database import Database; from sqlalchemy import text; \
print('DB OK rows:', Database().session_scope().__enter__().execute(text(\"select count(*) from legal_proceedings where county_id='pinellas'\")).scalar())"
which microsoft-edge-stable || ls $CF_BYPASS_BROWSER_PATH
```
Both must succeed before continuing.

---

## 2. Start a virtual display (CF requires headed Edge)

```bash
pkill -f "Xvfb :99" 2>/dev/null; sleep 1
Xvfb :99 -screen 0 1400x900x24 >/dev/null 2>&1 &
export DISPLAY=:99
```
Keep this `DISPLAY` exported for every later step (warming AND running).

---

## 3. Warm ONE Cloudflare profile **on this server's IP**

> ⚠️ `cf_clearance` is bound to IP + TLS fingerprint. A profile warmed on another
> machine will NOT work here. Warm it on this server.

The capture script is interactive (waits for Enter after the CF JS auto-clears).
Feed Enter automatically after a 30s clearance window:

```bash
rm -rf data/cf_session/edge_profile        # start clean
( sleep 30; printf '\n' ) | python scripts/experiments/cf_capture_pinellas_clerk.py
```

**GATE 2 — verify the warmed profile reaches the portal (not the CF wall):**
```bash
python scripts/experiments/cf_test_with_playwright.py
```
Expect: `VERDICT: Portal accessible`. If it shows the CF challenge / fails,
re-run step 3 once. If it fails again, STOP and report (the server IP may be
blocked or need a residential proxy).

---

## 4. Fan out profiles for N streams

The capture writes to `data/cf_session/edge_profile`; the launcher expects
`edge_profile_pin0 .. pin{N-1}`:

```bash
N=8
cp -r data/cf_session/edge_profile data/cf_session/edge_profile_pin0
for i in $(seq 1 $((N-1))); do
  cp -r data/cf_session/edge_profile_pin0 data/cf_session/edge_profile_pin$i
done
ls -d data/cf_session/edge_profile_pin*    # confirm pin0..pin7 exist
```

---

## 5. Smoke test 2 concurrent streams BEFORE the full run

```bash
python scripts/backfill_pinellas_case_numbers.py --profile pin0 --shard 0/2 --limit 2 > /tmp/s0.log 2>&1 &
python scripts/backfill_pinellas_case_numbers.py --profile pin1 --shard 1/2 --limit 2 > /tmp/s1.log 2>&1 &
wait
grep -E "candidates|-> |FAILED|DONE" /tmp/s0.log /tmp/s1.log | grep -v httpx
```
**GATE 3:** both logs must show `-> <UCN>` lines and `DONE: ... failed=0`. If you
see `Timeout` or `just a moment`, CF is rejecting concurrency from this IP —
reduce N (try 4) or add per-profile proxies before the full run.

---

## 6. Run the full backfill under screen

Start CONSERVATIVE, then scale. Run inside `screen` so it survives disconnect.

```bash
screen -S backfill          # (or: screen -dmS backfill bash scripts/run_backfill_parallel.sh 6)
export DISPLAY=:99
bash scripts/run_backfill_parallel.sh 6        # start with 6; raise to 8 if clean
# detach: Ctrl-A then D       reattach: screen -r backfill
```

The launcher already: starts one shared Xvfb if `DISPLAY` unset, validates the
pin profiles exist, staggers stream starts by 5s, logs to
`scratch/backfill_logs/stream_*.log`, and `wait`s for all to finish.

---

## 7. Monitor

```bash
tail -f scratch/backfill_logs/stream_*.log          # live
grep -hc "INFO - \[" scratch/backfill_logs/stream_*.log   # rows done per stream
```
DB progress / remaining:
```bash
python - <<'PY'
from src.core.database import Database
from sqlalchemy import text
db=Database()
with db.session_scope() as s:
    for st in ('found','duplicate','none'):
        n=s.execute(text("select count(*) from legal_proceedings where county_id='pinellas' and meta_data->>'casenumber_lookup'=:st"),{'st':st}).scalar()
        print('legal_proceedings', st, n)
    rem_lp=s.execute(text("select count(*) from legal_proceedings where county_id='pinellas' and record_type in ('Probate','Divorce') and case_number ~ '^[0-9]{10}$' and (meta_data->>'casenumber_lookup') is null")).scalar()
    rem_ll=s.execute(text("select count(*) from legal_and_liens where county_id='pinellas' and record_type='Judgment' and case_number is null and (meta_data->>'casenumber_lookup') is null")).scalar()
    print('REMAINING', rem_lp+rem_ll)
PY
```

---

## 8. Failure handling

- **A stream dies / `Timeout` spikes:** kill the screen (`screen -X -S backfill quit`),
  lower N, and re-run step 6. Resumable — done rows are skipped.
- **CF wall returns mid-run** (`just a moment`, repeated timeouts): the warmed
  clearance expired or the IP got flagged. Re-do step 3 (re-warm) + step 4
  (re-copy), then re-run. Lower N.
- **Transient row failures:** harmless — they stay unmarked and are retried on
  the next launcher run. After the main run, just re-run the launcher once to
  mop up stragglers.

---

## 9. Completion criteria

Re-run the launcher until **REMAINING == 0** (step 7), or until only a small,
stable failure tail remains (those rows had no Details/CASENUMBER). The script
prints `Backfill candidates: 0` per stream when nothing is left.

Final verification:
```sql
SELECT meta_data->>'casenumber_lookup' AS status, count(*)
FROM legal_proceedings WHERE county_id='pinellas' AND record_type IN ('Probate','Divorce')
GROUP BY 1
UNION ALL
SELECT 'judgment:'||coalesce(meta_data->>'casenumber_lookup','pending'), count(*)
FROM legal_and_liens WHERE county_id='pinellas' AND record_type='Judgment'
GROUP BY 1;
```
Expected end state: nearly all rows `found` or `duplicate`; a small `none` tail
(records with no court case on file) is normal.

---

## 10. Cleanup

```bash
screen -X -S backfill quit 2>/dev/null
pkill -f "Xvfb :99" 2>/dev/null
```

---

## Hard constraints (do NOT violate)

1. **Only ONE machine runs the backfill at a time.** Do not run here while the
   desktop (or another server) is running it — overlapping shards cause
   `case_number` unique collisions and wasted work. (The desktop streams are
   already stopped.)
2. **Never commit `.env`** or the `data/cf_session/` profiles.
3. **Do not change the shard count mid-run** on the same stream set; to resume,
   just re-run the launcher (done-markers make any N safe across runs).
4. This backfill **only reads CASENUMBER** — it does not download document images
   or touch the courtrecords docket portal. No 2captcha, no Anthropic spend.
5. The **duplicate** rows (many instruments → one estate case) are expected and
   intentionally left flagged, not merged. Dedup is a separate, later task
   (collapse to one row per case after relaxing the `case_number` global-unique
   to `(county_id, case_number)`). Do not attempt dedup here.
