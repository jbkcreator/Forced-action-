# Borrower lead score 0–10 (Runbook § 4.3, Item 3)

`src/lending/borrower_score.py` scores a borrower lead from 0 to 10 so callers can work the best leads first.
Points and thresholds live in `config/lending_borrower_score.py`.

| Criterion (Runbook § 4.3) | Points | Where the input comes from today |
|---|---|---|
| Under contract, live contract in hand | +4 | **Nowhere**: no lead source stores it, so it is always missing |
| 3+ completed flips / ground-up builds in the trailing 36 months | +3 | Website leads only (`web_leads.completed_projects_3y`, the form band) |
| Property in Florida or Georgia | +2 | LendingFlow leads only (`lendingflow_leads.property_state`) |
| Requested loan of $300K or more | +1 | LendingFlow leads only (`lendingflow_leads.loan_amount`, the low end of the band) |

Rules:
- A criterion earns its points only from a **known** input. An unknown input scores 0 and is listed in `missing`,
  never guessed. A known input that misses the threshold scores 0 and is not missing.
- Form bands are read by their lowest number: `1 to 2` → 1, `3 to 5` → 3, `6+` → 6; an answer starting "None"
  (the live form's `None yet`) → 0. A band with no number (for example "not sure") is missing.
- Live check (2026-10-19, read-only, counts only): 5 LendingFlow leads score 3 (four) and 2 (one); 9 website leads
  score 0 (form answers "None yet" ×6, "1 to 2" ×3).
- The result lists each criterion met with its points, so a caller can see why a lead scored what it did.

What this means today: a LendingFlow lead scores at most **3** (state + amount), a website lead at most **3**
(experience). 10 needs the "under contract" input, which no lead source collects yet.

Open:
- Q-17: where "under contract" and experience (for LendingFlow leads) come from.
- Q-18: whether this score replaces or sits beside the existing lead score (`src/lending/lead_scoring.py`), and
  how Dev 2's T-08 call queues read it (stored column or computed when read). The function is not wired into any
  queue yet.
