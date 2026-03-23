"""Quick one-off test for the welcome email. Delete after use."""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from types import SimpleNamespace
from src.services.stripe_webhooks import _send_welcome_email

# Mock subscriber — no DB needed
subscriber = SimpleNamespace(
    id=1,
    email="amal.antony@heu.ai",
    name="Test User",
    tier="pro",
    vertical="fix_and_flip",
    founding_member=True,
    event_feed_uuid="test-uuid-1234-abcd-5678",
)

from config.settings import get_settings
settings = get_settings()
print(f"SMTP host : {settings.smtp_host}")
print(f"SMTP user : {settings.smtp_user}")
print(f"Email from: {settings.email_from or settings.smtp_user}")
print(f"Sending to: {subscriber.email}")
print()

_send_welcome_email(subscriber)
print("Done — check your Mailtrap inbox.")
