# Tax-collector billing addresses patch absentee status; appraiser stays primary

**Status:** accepted

## Decision

Tax Collector billing addresses (Hillsborough "Public - Unpaid R/E Accounts"
`Billing Address`, Pinellas "Delinq Taxes-Certs Unpaid" `Owner Address`) are
ingested through the **existing tax-delinquency upload endpoint** — no new
upload source — and applied to `owners.absentee_status` via a **middle path**:

- write `absentee_status` only when **(a)** it is currently NULL, or
  **(b)** the tax-bill mailing address normalizes differently from the
  appraiser-derived `owners.mailing_address` — in which case the tax bill
  wins (the county demonstrably mails there) and the differing address is
  also written to `enriched_contacts(source='tax_collector')`;
- otherwise the appraiser value stands untouched.

Properties whose `absentee_status` changes **do trigger a CDS rescore**
(the existing absentee bonus: Out-of-State +15, Out-of-County +8).

The loader additionally splits rows by tax year: rows with Tax Yr **before**
the current roll year become `tax_delinquencies` distress rows; current-roll
rows (installment/prepay accounts, ~14k of 42.7k in the first Hillsborough
file) are used **only** for the billing-address comparison and never become
delinquency signals.

## Why

- **The appraiser path already covers all ~522k parcels** and computes
  `absentee_status` from site-vs-mailing comparison. The tax files cover only
  delinquent/unpaid accounts (~33k Hillsborough, ~4k Pinellas) — they cannot
  replace the appraiser; at best they patch gaps and staleness.
- **The tax bill is the freshest ground truth available.** Owners update the
  Tax Collector's billing address because money depends on it; appraiser
  mailing addresses lag. Where the two disagree, the tax bill is the better
  signal — measured yield on the first files: 2,249 Out-of-State + 2,248
  Out-of-County unique Hillsborough accounts, plus 11,443 in-county alternate
  mailings usable for the direct-mail fallback.
- **Current-roll rows are not distress.** An account marked Unpaid for a tax
  year that is not yet delinquent (installment participants) would pollute
  the tax-delinquency vertical and inflate CDS for owners who simply pay
  quarterly.

## Why this is surprising (read before "fixing" it)

Two traps for a future engineer:

1. The original task spec reads "TaxCollectorLoader … write absentee_status
   to the Owner model" — a naive read says *always overwrite*. Decision 1 of
   the design session locked the appraiser as authoritative; the middle path
   is the reconciliation. Do not "simplify" to a blind overwrite — you would
   clobber 522k appraiser-derived values with coverage from a 33k-row file.
2. The split looks like a missing feature ("why aren't the 2026 rows in
   `tax_delinquencies`?"). It is deliberate; see above.

## Considered alternatives

- **Tax collector authoritative (spec-literal overwrite).** Rejected:
  coverage is ~6% of parcels; one bad upload would corrupt absentee state
  county-wide.
- **Contact-enrichment only, no `absentee_status` writes, no rescore.**
  Rejected: forfeits the already-wired CDS absentee bonus exactly where the
  signal is strongest (delinquent + absentee).
- **Separate `tax_collector` upload source/endpoint.** Rejected: the billing
  address rides in files the tax-delinquency upload already accepts; a second
  upload of the same file would duplicate mapping profiles and operator steps.
