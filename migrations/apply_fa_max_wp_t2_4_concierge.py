"""FA Max WP-T2-4 — Reply Agent as Portal Concierge: schema migration.

Idempotent. Safe to re-run. Apply after WP-1 and WP-2 migrations.

Creates:
  fa_max_concierge_kb   — approved knowledge base topics (data-driven)
  fa_max_concierge_log  — full audit log for every classification + action

Seeds 5 approved KB topics confirmed by client (Q12).
"""
from __future__ import annotations

import sys

sys.path.insert(0, ".")
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from sqlalchemy import text
from src.core.database import get_db_context

STATEMENTS: list[tuple[str, str]] = [
    (
        "CREATE fa_max_concierge_kb",
        """
        CREATE TABLE IF NOT EXISTS fa_max_concierge_kb (
            id             SERIAL PRIMARY KEY,
            topic_key      VARCHAR(80)  NOT NULL UNIQUE,
            topic_label    VARCHAR(200) NOT NULL,
            answer_template TEXT        NOT NULL,
            is_active      BOOLEAN      NOT NULL DEFAULT TRUE,
            created_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
            updated_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW()
        );
        """,
    ),
    (
        "CREATE fa_max_concierge_log",
        """
        CREATE TABLE IF NOT EXISTS fa_max_concierge_log (
            id                  BIGSERIAL    PRIMARY KEY,
            person_id           UUID,
            opportunity_id      UUID,
            inbound_channel     VARCHAR(20)  NOT NULL
                CHECK (inbound_channel IN ('email', 'sms', 'slack', 'webhook')),
            inbound_snippet     TEXT,
            classification      VARCHAR(40)
                CHECK (classification IN (
                    'interested', 'not_interested', 'clarifying_question',
                    'wrong_person', 'opt_out', 'out_of_office', 'referral',
                    'pricing_escalate', 'below_threshold'
                )),
            kb_topic_key        VARCHAR(80),
            confidence          NUMERIC(4,3),
            action_taken        VARCHAR(40)
                CHECK (action_taken IN (
                    'kb_reply_sent', 'kb_reply_queued', 'escalated_exceptions',
                    'opt_out_suppressed', 'no_action'
                )),
            autonomy_tier       VARCHAR(1)   CHECK (autonomy_tier IN ('A', 'B', 'C')),
            relay_queue_id      INTEGER,
            created_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW()
        );
        """,
    ),
    (
        "INDEX fa_max_concierge_log.person_id",
        "CREATE INDEX IF NOT EXISTS ix_fa_max_concierge_log_person ON fa_max_concierge_log (person_id);",
    ),
    (
        "INDEX fa_max_concierge_log.created_at",
        "CREATE INDEX IF NOT EXISTS ix_fa_max_concierge_log_created ON fa_max_concierge_log (created_at DESC);",
    ),
    (
        "SEED KB topic: prequal_process",
        """
        INSERT INTO fa_max_concierge_kb (topic_key, topic_label, answer_template)
        VALUES (
            'prequal_process',
            'How the pre-qualification process works and what to expect',
            'Great question. The pre-qualification process with Backflip takes about 10 minutes. '
            'You''ll answer a few questions about the property and your experience, and Backflip '
            'will let you know within 24 hours whether the deal fits their current program. '
            'Happy to walk you through what to expect — would a quick call help?'
        )
        ON CONFLICT (topic_key) DO NOTHING;
        """,
    ),
    (
        "SEED KB topic: documents_needed",
        """
        INSERT INTO fa_max_concierge_kb (topic_key, topic_label, answer_template)
        VALUES (
            'documents_needed',
            'What documents are typically needed',
            'For a typical hard money deal through Backflip, you''ll generally need: '
            'a signed purchase contract, your entity docs (if buying in an LLC), '
            'a scope of work or rehab budget, and photos or an inspection report if available. '
            'Backflip may ask for additional items based on the deal — they''ll let you know '
            'exactly what they need once you submit. Anything specific you''re unsure about?'
        )
        ON CONFLICT (topic_key) DO NOTHING;
        """,
    ),
    (
        "SEED KB topic: eligible_property_loan_types",
        """
        INSERT INTO fa_max_concierge_kb (topic_key, topic_label, answer_template)
        VALUES (
            'eligible_property_loan_types',
            'What property and loan types are eligible',
            'Backflip primarily does fix-and-flip loans on non-owner-occupied residential properties — '
            'single-family, duplexes, small multifamily. They also do new construction and '
            'ground-up projects in select markets. They don''t do primary residences or commercial. '
            'If you have a specific property type in mind, I can get you a quick read on whether '
            'it''s likely to fit.'
        )
        ON CONFLICT (topic_key) DO NOTHING;
        """,
    ),
    (
        "SEED KB topic: funding_timeline",
        """
        INSERT INTO fa_max_concierge_kb (topic_key, topic_label, answer_template)
        VALUES (
            'funding_timeline',
            'Typical funding timeline once a deal is submitted',
            'Once a complete deal is submitted through Backflip''s portal, they typically '
            'issue a term sheet within 24–48 hours. From term sheet to funding usually runs '
            '7–14 business days, depending on title and any open conditions. '
            'If you''re under contract with a tight close date, let me know and I''ll flag it '
            'so we can prioritize.'
        )
        ON CONFLICT (topic_key) DO NOTHING;
        """,
    ),
    (
        "SEED KB topic: referral_relationship",
        """
        INSERT INTO fa_max_concierge_kb (topic_key, topic_label, answer_template)
        VALUES (
            'referral_relationship',
            'How the referral or deal-flow relationship works',
            'Simple setup — I introduce qualified deals to Backflip on your behalf. '
            'You apply directly through their portal, they review and fund the deal, '
            'and I stay involved to help move things along if anything gets stuck. '
            'No cost to you for the referral. Want me to walk you through submitting your first deal?'
        )
        ON CONFLICT (topic_key) DO NOTHING;
        """,
    ),
]


def main() -> None:
    with get_db_context() as db:
        for label, sql in STATEMENTS:
            try:
                db.execute(text(sql))
                db.commit()
                print(f"  OK  {label}")
            except Exception as exc:
                db.rollback()
                print(f"  ERR {label}: {exc}")
                raise


if __name__ == "__main__":
    main()
