"""
CLONE-v2.2 — fleet-wide widening of src/services/playbook_writer.py.

Covers: default 'lifecycle' behavior stays byte-identical (no agent_domain
prefix, no entry_kind branch change), non-lifecycle agent_domain namespacing
so Vera/Cora/Hunter/fleet entries can never collide with Lifecycle's or each
other's, entry_kind validation, and a 3+ anti-playbook-entry scenario per the
fleet constitutions' "anti-playbooks at 3+ failures" rule.
"""

from __future__ import annotations

import uuid

import pytest

from src.core.models import LifecyclePlaybook
from src.services.playbook_writer import (
    mark_contradicted,
    supersede_recommendation,
    upsert_recommendation,
)


def _cleanup(db, source_id: str) -> None:
    db.query(LifecyclePlaybook).filter_by(source_id=source_id).delete()
    db.commit()


def test_default_domain_matches_pre_widening_behavior(fresh_db):
    """No agent_domain/entry_kind kwargs -> 'lifecycle'/'playbook', source_key
    unprefixed, exactly as every existing caller (ab_engine, self_healing,
    holdout_check) already relies on."""
    source_id = f"test_{uuid.uuid4().hex[:8]}"
    try:
        new_id = upsert_recommendation(
            fresh_db,
            name="control-lever winner", description="A/B winner promotion",
            pattern={"variant": "b"}, source_type="ab_test", source_id=source_id,
            authored_by="lifecycle",
        )
        assert new_id is not None

        row = fresh_db.query(LifecyclePlaybook).filter_by(id=new_id).one()
        assert row.agent_domain == "lifecycle"
        assert row.entry_kind == "playbook"
        assert row.source_key == f"ab_test:{source_id}"
    finally:
        _cleanup(fresh_db, source_id)


def test_non_lifecycle_domain_is_namespaced_in_source_key(fresh_db):
    """Vera and Hunter writing under the same source_type/source_id must not
    collide with each other or with an unscoped 'lifecycle' entry."""
    source_id = f"shared_{uuid.uuid4().hex[:8]}"
    try:
        lifecycle_id = upsert_recommendation(
            fresh_db,
            name="lifecycle entry", description="baseline", pattern={},
            source_type="pattern_match", source_id=source_id, authored_by="lifecycle",
        )
        vera_id = upsert_recommendation(
            fresh_db,
            name="vera entry", description="vera's own finding", pattern={},
            source_type="pattern_match", source_id=source_id, authored_by="vera",
            agent_domain="vera",
        )
        hunter_id = upsert_recommendation(
            fresh_db,
            name="hunter entry", description="hunter's own finding", pattern={},
            source_type="pattern_match", source_id=source_id, authored_by="hunter",
            agent_domain="hunter",
        )

        assert lifecycle_id is not None
        assert vera_id is not None
        assert hunter_id is not None
        assert len({lifecycle_id, vera_id, hunter_id}) == 3

        rows = {
            r.agent_domain: r
            for r in fresh_db.query(LifecyclePlaybook).filter_by(source_id=source_id).all()
        }
        assert rows["lifecycle"].source_key == f"pattern_match:{source_id}"
        assert rows["vera"].source_key == f"vera:pattern_match:{source_id}"
        assert rows["hunter"].source_key == f"hunter:pattern_match:{source_id}"
    finally:
        _cleanup(fresh_db, source_id)


def test_same_domain_dedupes_on_second_call(fresh_db):
    """Two calls with the same (agent_domain, source_type, source_id) ->
    only one row; the second returns None (ON CONFLICT DO NOTHING)."""
    source_id = f"dedupe_{uuid.uuid4().hex[:8]}"
    try:
        first_id = upsert_recommendation(
            fresh_db,
            name="cora finding", description="first write", pattern={},
            source_type="metric_breach", source_id=source_id, authored_by="cora",
            agent_domain="cora",
        )
        second_id = upsert_recommendation(
            fresh_db,
            name="cora finding retry", description="second write", pattern={},
            source_type="metric_breach", source_id=source_id, authored_by="cora",
            agent_domain="cora",
        )

        assert first_id is not None
        assert second_id is None
        assert fresh_db.query(LifecyclePlaybook).filter_by(
            agent_domain="cora", source_type="metric_breach", source_id=source_id,
        ).count() == 1
    finally:
        _cleanup(fresh_db, source_id)


def test_invalid_entry_kind_raises(fresh_db):
    with pytest.raises(ValueError):
        upsert_recommendation(
            fresh_db,
            name="bad kind", description="should not write", pattern={},
            source_type="ab_test", source_id="irrelevant", authored_by="lifecycle",
            entry_kind="not_a_real_kind",
        )


def test_anti_playbook_category_holds_three_plus_documented_failures(fresh_db):
    """Per docs/constitutions/*.md: 'anti-playbooks at 3+ failures'. Writes
    three distinct anti_playbook entries for Hunter and confirms they land
    in the anti_playbook category, independent of any playbook entries."""
    base = f"antiplaybook_{uuid.uuid4().hex[:8]}"
    failure_source_ids = [f"{base}_{i}" for i in range(3)]
    try:
        ids = [
            upsert_recommendation(
                fresh_db,
                name=f"hunter failure {i}", description="documented dead-end approach",
                pattern={"attempt": i}, source_type="lead_source_dead_end",
                source_id=sid, authored_by="hunter", agent_domain="hunter",
                entry_kind="anti_playbook",
            )
            for i, sid in enumerate(failure_source_ids)
        ]
        assert all(i is not None for i in ids)

        rows = fresh_db.query(LifecyclePlaybook).filter(
            LifecyclePlaybook.agent_domain == "hunter",
            LifecyclePlaybook.entry_kind == "anti_playbook",
            LifecyclePlaybook.source_id.in_(failure_source_ids),
        ).all()
        assert len(rows) == 3
        assert all(r.entry_kind == "anti_playbook" for r in rows)
    finally:
        for sid in failure_source_ids:
            _cleanup(fresh_db, sid)


# ─────────────────────────────────────────────────────────────────────────────
# LEARN-v2.2 Layer 4 (Step 11) — lesson versioning/confidence/scope,
# supersede_recommendation(), mark_contradicted()
# ─────────────────────────────────────────────────────────────────────────────

def test_confidence_and_scope_default_to_null_version_defaults_to_one(fresh_db):
    source_id = f"lesson_default_{uuid.uuid4().hex[:8]}"
    try:
        new_id = upsert_recommendation(
            fresh_db,
            name="no confidence/scope given", description="baseline call, no new kwargs",
            pattern={}, source_type="ab_test", source_id=source_id, authored_by="lifecycle",
        )
        row = fresh_db.query(LifecyclePlaybook).filter_by(id=new_id).one()
        assert row.confidence is None
        assert row.scope is None
        assert row.version == 1
        assert row.superseded_by_id is None
    finally:
        _cleanup(fresh_db, source_id)


def test_confidence_and_scope_persist_when_given(fresh_db):
    source_id = f"lesson_scoped_{uuid.uuid4().hex[:8]}"
    try:
        new_id = upsert_recommendation(
            fresh_db,
            name="ROI framing wins for buy-and-hold",
            description="ROI-focused messaging outperforms urgency framing",
            pattern={"angle": "roi"}, source_type="experiment_verdict", source_id=source_id,
            authored_by="fleet", agent_domain="fleet", entry_kind="playbook",
            confidence=82, scope={"buyer_type": "buy_and_hold", "offer": "founder_tier"},
        )
        row = fresh_db.query(LifecyclePlaybook).filter_by(id=new_id).one()
        assert row.confidence == 82
        assert row.scope == {"buyer_type": "buy_and_hold", "offer": "founder_tier"}
    finally:
        _cleanup(fresh_db, source_id)


def test_confidence_out_of_range_rejected_by_db_constraint(fresh_db):
    source_id = f"lesson_badconf_{uuid.uuid4().hex[:8]}"
    with pytest.raises(Exception):
        upsert_recommendation(
            fresh_db,
            name="bad confidence", description="should violate check constraint",
            pattern={}, source_type="ab_test", source_id=source_id, authored_by="lifecycle",
            confidence=150,
        )
    fresh_db.rollback()


def test_supersede_recommendation_marks_old_row_and_links_replacement(fresh_db):
    old_source_id = f"lesson_old_{uuid.uuid4().hex[:8]}"
    new_source_id = f"lesson_new_{uuid.uuid4().hex[:8]}"
    try:
        old_id = upsert_recommendation(
            fresh_db,
            name="urgency framing (v1)", description="superseded by a later, better-evidenced angle",
            pattern={"angle": "urgency"}, source_type="experiment_verdict", source_id=old_source_id,
            authored_by="fleet", agent_domain="fleet",
        )
        new_id = upsert_recommendation(
            fresh_db,
            name="roi framing (v2)", description="beat urgency framing on a larger sample",
            pattern={"angle": "roi"}, source_type="experiment_verdict", source_id=new_source_id,
            authored_by="fleet", agent_domain="fleet",
        )

        updated = supersede_recommendation(fresh_db, old_id, new_id)
        assert updated is True

        old_row = fresh_db.query(LifecyclePlaybook).filter_by(id=old_id).one()
        assert old_row.status == "superseded"
        assert old_row.superseded_by_id == new_id
    finally:
        _cleanup(fresh_db, old_source_id)
        _cleanup(fresh_db, new_source_id)


def test_supersede_recommendation_no_op_on_already_terminal_row(fresh_db):
    """A rejected/retired row is a closed human decision — a later
    experiment superseding it silently would hide that it was overridden."""
    source_id = f"lesson_terminal_{uuid.uuid4().hex[:8]}"
    try:
        old_id = upsert_recommendation(
            fresh_db,
            name="already rejected", description="human already said no",
            pattern={}, source_type="experiment_verdict", source_id=source_id,
            authored_by="fleet", agent_domain="fleet",
        )
        fresh_db.execute(
            LifecyclePlaybook.__table__.update()
            .where(LifecyclePlaybook.id == old_id)
            .values(status="rejected")
        )
        fresh_db.flush()

        updated = supersede_recommendation(fresh_db, old_id, new_id=999999)
        assert updated is False

        row = fresh_db.query(LifecyclePlaybook).filter_by(id=old_id).one()
        assert row.status == "rejected"
        assert row.superseded_by_id is None
    finally:
        _cleanup(fresh_db, source_id)


def test_mark_contradicted_sets_terminal_state(fresh_db):
    source_id = f"lesson_contradicted_{uuid.uuid4().hex[:8]}"
    try:
        new_id = upsert_recommendation(
            fresh_db,
            name="claim later contradicted", description="3+ counter-instances found",
            pattern={}, source_type="experiment_verdict", source_id=source_id,
            authored_by="fleet", agent_domain="fleet",
        )
        updated = mark_contradicted(fresh_db, new_id)
        assert updated is True

        row = fresh_db.query(LifecyclePlaybook).filter_by(id=new_id).one()
        assert row.status == "contradicted"
    finally:
        _cleanup(fresh_db, source_id)
