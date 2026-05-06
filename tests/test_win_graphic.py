"""
Win-graphic + Social-proof wall — Stage 5 — unit + integration tests.

Run:
    pytest tests/test_win_graphic.py -v
"""

from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch
import uuid

import pytest

from src.core.models import DealOutcome, Subscriber


class TestProofWallPayloadShape:
    def test_skip_buckets_excluded(self, fresh_db):
        from src.services.win_graphic import proof_wall_payload

        uid = uuid.uuid4().hex[:8]
        sub = Subscriber(
            stripe_customer_id=f"cus_proofwall_{uid}",
            tier="starter", vertical="roofing", county_id="hillsborough",
            event_feed_uuid=f"proof-{uid}",
            name="Alex Smith",
        )
        fresh_db.add(sub)
        fresh_db.flush()

        good = DealOutcome(
            subscriber_id=sub.id,
            deal_size_bucket="10_25k",
            deal_amount=15000,
            deal_date=date.today(),
        )
        skip = DealOutcome(
            subscriber_id=sub.id,
            deal_size_bucket="skip",
            deal_date=date.today(),
        )
        fresh_db.add_all([good, skip])
        fresh_db.flush()

        items = proof_wall_payload(fresh_db, limit=50)
        ids = [i["deal_outcome_id"] for i in items]
        assert good.id in ids
        assert skip.id not in ids   # skip-bucket suppressed

    def test_payload_anonymized(self, fresh_db):
        """No subscriber name, no exact deal amount, no address — just bucket/vertical/county."""
        from src.services.win_graphic import proof_wall_payload

        uid = uuid.uuid4().hex[:8]
        sub = Subscriber(
            stripe_customer_id=f"cus_anon_{uid}",
            tier="starter", vertical="roofing", county_id="hillsborough",
            event_feed_uuid=f"anon-{uid}",
            name="Should Not Leak",
            email="should.not.leak@example.com",
        )
        fresh_db.add(sub)
        fresh_db.flush()
        deal = DealOutcome(
            subscriber_id=sub.id,
            deal_size_bucket="25k_plus",
            deal_amount=42000,
            deal_date=date.today(),
        )
        fresh_db.add(deal)
        fresh_db.flush()

        items = proof_wall_payload(fresh_db, limit=50)
        match = [i for i in items if i["deal_outcome_id"] == deal.id][0]

        # PII guarantees
        flat = str(match)
        assert "Should Not Leak" not in flat
        assert "should.not.leak@example.com" not in flat
        assert "42000" not in flat
        # Required public fields present
        assert match["deal_size_bucket"] == "25k_plus"
        assert match["vertical"] == "roofing"
        assert match["county_id"] == "hillsborough"
        assert match["graphic_url"].endswith(f"/api/win-graphic/{deal.id}")
        assert match["days_ago"] == 0


class TestGenerateGraceful:
    def test_missing_deal_returns_none(self):
        from src.services import win_graphic
        db = MagicMock()
        db.get.return_value = None
        out = win_graphic.generate(deal_outcome_id=999, db=db)
        assert out is None

    def test_pillow_missing_returns_none(self):
        """If Pillow can't be imported, generate() returns None instead of raising."""
        import builtins
        real_import = builtins.__import__

        def fake_import(name, *args, **kwargs):
            if name == "PIL":
                raise ImportError("Pillow not installed")
            return real_import(name, *args, **kwargs)

        from src.services import win_graphic
        db = MagicMock()
        with patch.object(builtins, "__import__", side_effect=fake_import):
            out = win_graphic.generate(deal_outcome_id=42, db=db)
        assert out is None


class TestGenerateWritesPng:
    def test_writes_real_png(self, fresh_db, tmp_path, monkeypatch):
        """End-to-end: create a deal, render the graphic, verify a PNG exists."""
        pytest.importorskip("PIL")
        from src.services import win_graphic

        uid = uuid.uuid4().hex[:8]
        sub = Subscriber(
            stripe_customer_id=f"cus_gfx_{uid}",
            tier="starter", vertical="restoration", county_id="hillsborough",
            event_feed_uuid=f"gfx-{uid}",
            name="Test Renderer",
        )
        fresh_db.add(sub)
        fresh_db.flush()
        deal = DealOutcome(
            subscriber_id=sub.id,
            deal_size_bucket="10_25k",
            deal_amount=12500,
            deal_date=date.today(),
        )
        fresh_db.add(deal)
        fresh_db.flush()

        # Redirect output to a temp dir so we don't litter the repo
        monkeypatch.setattr(win_graphic, "_OUTPUT_DIR", tmp_path)
        out = win_graphic.generate(deal.id, fresh_db)
        assert out is not None
        assert Path(out).exists()
        # Sanity: PNG header
        with open(out, "rb") as fh:
            header = fh.read(8)
        assert header.startswith(b"\x89PNG")
