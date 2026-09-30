"""Per-borrower Backflip conflict check: match rules.

Email-domain matching is off until the client decides how free-mail domains
(gmail.com, yahoo.com, ...) are treated; matching every domain would flag
every borrower who shares a free-mail provider with any Backflip contact.
"""

MATCH_EMAIL_DOMAIN: bool = False
