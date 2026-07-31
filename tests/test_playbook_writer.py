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
from src.services.playbook_writer import upsert_recommendation


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
