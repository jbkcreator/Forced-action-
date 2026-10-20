"""Retargeting audience export (Runbook § 4.7, Item 7): nightly hashed contact files for Google Customer Match and
Meta Custom Audiences, staged on the server for later ad activation. Nothing is uploaded to the ad platforms.

Off until LENDING_AUDIENCE_EXPORT_ENABLED=true and LENDING_AUDIENCE_EXPORT_DIR is set (config/settings.py).
"""
from __future__ import annotations

# Nightly files kept per platform; older ones are deleted after each run.
KEEP_NIGHTS = 7

GOOGLE_FILE = "google_customer_match_{day}.csv"
META_FILE = "meta_custom_audience_{day}.csv"

# Column headers of each platform's contact-list upload template, holding SHA-256 hex values only.
GOOGLE_HEADERS = ("Email", "Phone")
META_HEADERS = ("email", "phone")

# Google Customer Match normalisation: periods before the domain are removed for these domains before hashing.
GOOGLE_DOTLESS_DOMAINS = frozenset({"gmail.com", "googlemail.com"})
