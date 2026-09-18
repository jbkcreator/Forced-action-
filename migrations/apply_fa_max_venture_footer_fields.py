"""FA Max WP-T2-1 go-live review — venture footer fields + Josh's confirmed
footer text for fa_max_lending (SOT.md client Q9 "Email Branding, Signature,
and Compliance Footer").

Idempotent. Safe to re-run (ADD COLUMN IF NOT EXISTS; the seed UPDATE writes
the same literal values every run).

What this does:
1. Adds outbound_contact_phone / outbound_disclaimer to ventures (rendered
   in the CAN-SPAM footer alongside the existing brand_name/postal_address
   — see src/services/relay/channels_email.py:build_passthrough_body).
2. Updates the fa_max_lending venture row's brand_name/postal_address/
   outbound_contact_phone/outbound_disclaimer to Josh's own confirmed draft
   text from SOT.md Part 8 Q9: "Josh Kantor, Forced Action / 1320 W. Lemon
   St., Tampa, FL 33606 / (813) 361-8927 / This message is not an offer of
   credit and is not a solicitation to originate a loan. Reply STOP to opt
   out of future messages." The prior seed value ('Backflip') was a
   placeholder from before this was confirmed.

NOT included, because Josh explicitly flagged both as still open in that
same response: a title line under his name, and whether Backflip's
compliance team requires their own separate disclaimer added on top. Do
not guess either — ship this once, then append/adjust when he answers.
"""
from __future__ import annotations

from sqlalchemy import text

from src.core.database import get_db_context

CONFIRMED_BRAND_NAME = "Josh Kantor, Forced Action"
CONFIRMED_POSTAL_ADDRESS = "1320 W. Lemon St., Tampa, FL 33606"
CONFIRMED_PHONE = "(813) 361-8927"
CONFIRMED_DISCLAIMER = (
    "This message is not an offer of credit and is not a solicitation to "
    "originate a loan. Reply STOP to opt out of future messages."
)

STATEMENTS: list[tuple[str, str]] = [
    (
        "ADD outbound_contact_phone to ventures",
        "ALTER TABLE ventures ADD COLUMN IF NOT EXISTS outbound_contact_phone VARCHAR(30);",
    ),
    (
        "ADD outbound_disclaimer to ventures",
        "ALTER TABLE ventures ADD COLUMN IF NOT EXISTS outbound_disclaimer TEXT;",
    ),
    (
        "SEED fa_max_lending confirmed footer text",
        """
        UPDATE ventures
        SET brand_name = :brand_name,
            postal_address = :postal_address,
            outbound_contact_phone = :phone,
            outbound_disclaimer = :disclaimer
        WHERE venture_key = 'fa_max_lending';
        """,
    ),
]


def main() -> None:
    with get_db_context() as session:
        for label, sql in STATEMENTS:
            print(f"Applying: {label}")
            if "SEED fa_max_lending" in label:
                session.execute(
                    text(sql),
                    {
                        "brand_name": CONFIRMED_BRAND_NAME,
                        "postal_address": CONFIRMED_POSTAL_ADDRESS,
                        "phone": CONFIRMED_PHONE,
                        "disclaimer": CONFIRMED_DISCLAIMER,
                    },
                )
            else:
                session.execute(text(sql))
        session.commit()

        row = session.execute(
            text(
                "SELECT brand_name, postal_address, outbound_contact_phone, outbound_disclaimer "
                "FROM ventures WHERE venture_key = 'fa_max_lending'"
            )
        ).mappings().first()
        print("fa_max_lending venture footer fields:")
        print(f"  brand_name:             {row['brand_name'] if row else '<no row>'}")
        print(f"  postal_address:         {row['postal_address'] if row else None}")
        print(f"  outbound_contact_phone: {row['outbound_contact_phone'] if row else None}")
        print(f"  outbound_disclaimer:    {row['outbound_disclaimer'] if row else None}")


if __name__ == "__main__":
    main()
