# Dynamic Action Sequence Architecture

## Problem

Scraping the same data from multiple counties means the same logical steps — fill a date range, submit, wait for results, export or paginate — but each county portal has a different DOM. Three approaches exist, each with a hard trade-off:

| Approach | Cost per run | Handles quirks | Config burden |
|---|---|---|---|
| `browser_use` (LLM live) | High — LLM API call every run | Yes | None |
| Hardcoded role names | Zero | No — rigid 7-key mapping | Low |
| This approach | Zero after first run | Yes | Low |

The hardcoded role name approach (`start_date_input`, `export_btn`, etc.) is fast but can't handle portals that need JS injection, iframe navigation, a wait between steps, or any deviation from the fixed sequence. The browser-use approach handles everything but costs LLM tokens on every single daily scrape run.

---

## Solution: Generate Once, Cache, Self-Heal

The LLM generates a **JSON action sequence** for each county source once. Playwright interprets and executes that sequence on every subsequent run — no LLM involved. When the sequence fails (portal DOM changed), the engine detects it, clears the cached sequence, and regenerates on the next run.

```
Admin configures county source:
  URL + description + selectors (CSS selectors found in DevTools)
        ↓
First scrape run for that source:
  LLM sees URL + selectors + intent description
  → generates action_sequence JSON
  → stored in county_sources.special_flags.action_sequence
        ↓
All subsequent runs:
  Playwright reads action_sequence from DB
  → executes each step directly (no LLM call)
        ↓
Step fails (stale selector, DOM change):
  Engine detects error
  → clears action_sequence from DB
  → falls back to browser_use for this run
  → next run regenerates the sequence fresh
```

---

## Action Sequence Schema

The sequence is a JSON array of step objects. Each step has an `op` field and op-specific params.

```json
[
  { "op": "goto",          "url": "{url}" },
  { "op": "wait_load",     "state": "networkidle" },
  { "op": "click",         "selector": "a#searchLink" },
  { "op": "fill",          "selector": "#startDate",       "value": "{start_date}" },
  { "op": "fill",          "selector": "#endDate",         "value": "{end_date}" },
  { "op": "click",         "selector": "#searchBtn" },
  { "op": "wait_selector", "selector": ".ACA_Grid_OverFlow" },
  { "op": "download",      "selector": "#exportBtn" }
]
```

For extract (paginate + scrape rows):
```json
[
  { "op": "goto",          "url": "{url}" },
  { "op": "fill",          "selector": "#startDate",       "value": "{start_date}" },
  { "op": "fill",          "selector": "#endDate",         "value": "{end_date}" },
  { "op": "click",         "selector": "#searchBtn" },
  { "op": "wait_selector", "selector": ".ACA_Grid_OverFlow" },
  { "op": "extract_rows",  "selector": "tr.ACA_Row_Odd, tr.ACA_Row_Even",
                           "next_btn": "a[title='Go to next page']" }
]
```

### Supported ops

| `op` | Params | Playwright call |
|---|---|---|
| `goto` | `url` | `page.goto(url)` |
| `click` | `selector` | `page.click(selector)` |
| `fill` | `selector`, `value` | `page.fill(selector, value)` |
| `wait_selector` | `selector`, `timeout?` | `page.wait_for_selector(selector)` |
| `wait_load` | `state?` (default `networkidle`) | `page.wait_for_load_state(state)` |
| `wait_ms` | `ms` | `asyncio.sleep(ms / 1000)` |
| `js_eval` | `script` | `page.evaluate(script)` |
| `download` | `selector` | `page.click(selector)` + `page.expect_download()` |
| `extract_rows` | `selector`, `next_btn?` | paginate loop, scrape all rows into DataFrame |

`value` in `fill` ops supports `{start_date}`, `{end_date}`, and `{url}` placeholders — substituted at runtime.

`js_eval` is available for edge cases (dismissing cookie banners, scrolling into view, triggering a hidden input) but is generated only when the LLM determines it's necessary.

---

## Config Flow

### What the admin sets (in Special Flags JSON)

```json
{
  "scrape_mode": "selector",
  "selectors": {
    "start_date_input": "#ctl00...txtGSStartDate",
    "end_date_input":   "#ctl00...txtGSEndDate",
    "search_btn":       "#ctl00...btnNewSearch",
    "results_table":    ".ACA_Grid_OverFlow",
    "export_btn":       "#ctl00...ExportToExcel"
  }
}
```

The `selectors` dict is a human-readable hint — key names describe intent, values are CSS selectors found with browser DevTools. The admin never touches `action_sequence`.

### What gets stored after first generation

```json
{
  "scrape_mode": "selector",
  "selectors": { ... },
  "action_sequence": [
    { "op": "goto", "url": "{url}" },
    { "op": "fill", "selector": "#ctl00...txtGSStartDate", "value": "{start_date}" },
    ...
  ]
}
```

`action_sequence` is written back to `special_flags` via the existing `PATCH /admin/counties/{id}/sources/{src_id}` endpoint.

---

## Generation Prompt

The LLM receives:

```
You generate Playwright action sequences for a web scraping engine.
Given a portal URL, a description, and a dict of CSS selectors with their roles,
produce a JSON array of steps that will:
  1. Navigate to the portal
  2. Fill in start_date and end_date
  3. Submit the search
  4. Either click an export button (download mode) OR paginate through all result
     pages and extract rows (extract mode)

Return ONLY the JSON array. No explanation. No markdown fences.

Portal URL: {url}
Description: {description}
Navigation hint: {navigation_hint}
Selectors (role → CSS selector):
{selectors_json}
Start date placeholder: {start_date}   (MM/DD/YYYY)
End date placeholder:   {end_date}     (MM/DD/YYYY)
```

The response is parsed as JSON and validated against the known op list. Unknown ops are dropped. If the JSON is malformed or empty, the engine falls back to browser-use for this run (no sequence is cached).

---

## Engine Interpreter (pseudo-code)

```python
async def execute_action_sequence(sequence, page, context, placeholders):
    for step in sequence:
        op = step["op"]
        try:
            if op == "goto":
                url = step["url"].format(**placeholders)
                await page.goto(url, wait_until="networkidle", timeout=30_000)

            elif op == "fill":
                value = step["value"].format(**placeholders)
                await page.fill(step["selector"], value)

            elif op == "click":
                await page.click(step["selector"])

            elif op == "wait_selector":
                await page.wait_for_selector(
                    step["selector"],
                    timeout=step.get("timeout", 30_000)
                )

            elif op == "wait_load":
                await page.wait_for_load_state(step.get("state", "networkidle"))

            elif op == "wait_ms":
                await asyncio.sleep(step["ms"] / 1000)

            elif op == "js_eval":
                await page.evaluate(step["script"])

            elif op == "download":
                async with page.expect_download(timeout=60_000) as dl:
                    await page.click(step["selector"])
                download = await dl.value
                # save and return path ...

            elif op == "extract_rows":
                # paginate loop, collect rows, return DataFrame ...

        except Exception as e:
            raise ActionSequenceError(f"Step {op!r} failed: {e}") from e
```

`ActionSequenceError` propagates to the caller, which:
1. Logs the failed step
2. Writes `action_sequence: null` back to DB
3. Falls back to browser_use for this run

---

## Scrape Mode Routing (updated)

```
scrape_mode = source["scrape_mode"]   # download_direct | selector | extract | download

if scrape_mode == "download_direct":
    → direct HTTP, no browser

elif scrape_mode == "selector":
    if action_sequence in special_flags:
        → execute cached sequence with Playwright interpreter
    else:
        → call LLM to generate sequence
        → store sequence in DB
        → execute sequence
    on ActionSequenceError:
        → clear sequence from DB
        → fall back to browser_use (download sub-mode)

elif scrape_mode == "extract":
    → browser_use agent, extract mode (LLM reads table rows)

elif scrape_mode == "download":
    → browser_use agent, download mode (LLM finds export button)
```

---

## Why Not Raw Code Execution

The obvious alternative is asking the LLM to generate Python/Playwright code and running it with `exec()`. This is rejected because:

1. **Security**: if the DB is ever compromised, an attacker can inject arbitrary code that runs in the scraper process
2. **Non-determinism**: same intent prompt may produce slightly different code each call — harder to diff and debug
3. **No validation layer**: raw code can call anything; the action sequence op list is an explicit allowlist

The action sequence is a constrained DSL. The LLM can only produce steps from the known op table. Anything outside that is ignored.

---

## Self-Healing Cycle

```
Normal run:
  sequence exists → execute → success → no change

Portal updated (DOM changed):
  sequence exists → execute → step fails (selector not found)
  → clear action_sequence from special_flags
  → run browser_use fallback (succeeds)
  → log: "Sequence cleared — will regenerate on next run"

Next run:
  sequence missing → generate fresh from current selectors
  → execute → success
  → store new sequence

Selectors also stale:
  → generation produces sequence but execution still fails
  → same clear + fallback cycle
  → admin is alerted (scraper_stats records the fallback event)
  → admin updates selectors in UI → next run generates valid sequence
```

---

## Implementation Order

1. `src/utils/action_sequence.py` — interpreter (`execute_action_sequence`) + generator (`generate_action_sequence` calling Claude)
2. Update `permit_engine.py` `selector` branch to use the interpreter (replace current `_scrape_selector`)
3. Add `_clear_action_sequence(county_id, source_id)` helper that PATCHes the source record in DB
4. Roll out to `violation_engine.py`, `lien_engine.py`, and remaining engines
5. Admin UI: show a "Sequence cached" badge on sources that have `action_sequence` in `special_flags`; add a "Regenerate" button that clears it

---

## Key Properties

- **Zero per-run LLM cost** after first generation
- **Portal changes are handled automatically** — sequence self-invalidates on failure
- **Admin config stays simple** — just CSS selectors, no knowledge of the op schema required
- **Safe** — LLM output is validated against an explicit op allowlist, no `exec()`
- **Debuggable** — cached sequence is plain JSON visible in the DB or admin UI
- **County-agnostic** — same engine, same interpreter, different sequence per county
