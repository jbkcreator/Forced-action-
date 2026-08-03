"""
Tests for the lesson-hygiene sweep (LEARN-v2.2 Layer 4, Step 12).

Two tiers, deliberately:

  * decide() is pure, so every threshold combination and every safety rule is
    a table-driven unit test with no DB and no clock mocking. That is the
    whole reason the decision was factored out of the sweep.
  * The DB tier only covers what cannot be tested without Postgres — the
    aggregate query's FILTER/LATERAL semantics, the self-poisoning guard, the
    locking re-check, and idempotency.

The DB tier applies the LEARN Layer 4 DDL inside the test's own rolled-back
transaction rather than requiring the migration to have been run. Postgres DDL
is transactional, so the shared schema is untouched — and it means these tests
pass on an un-migrated environment, which is exactly the environment the
sweep's schema rail exists for.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import text as sa_text

from config import learning_hygiene as cfg
from config.settings import get_settings
from src.services import learning_hygiene as svc
from src.services.learning_hygiene import LessonStats, decide
from src.tasks import learning_hygiene_sweep as task_mod
from src.tasks.learning_hygiene_sweep import _VERDICT_LABELS, format_digest, run

NOW = datetime(2026, 8, 3, 12, 0, 0, tzinfo=timezone.utc)


# ── helpers ──────────────────────────────────────────────────────────────


def _stats(**overrides) -> LessonStats:
    """A healthy, measurable, freshly-exercised lesson. Override one thing."""
    base = {
        "lesson_id": 1,
        "name": "ab_winner:test_alpha",
        "status": "adopted",
        "entry_kind": "playbook",
        "agent_domain": "lifecycle",
        "source_type": "ab_test",
        "source_id": "test_alpha",
        "authored_at": NOW - timedelta(days=10),
        "confidence": None,
        "source_resolved": True,
        "contradictions": 0,
        "supports": 10,
        "non_evidence": 0,
        "last_evidence_at": NOW - timedelta(days=1),
        "successor_id": None,
    }
    base.update(overrides)
    return LessonStats(**base)


# ══════════════════════════════════════════════════════════════════════════
# Tier 1 — pure decide()
# ══════════════════════════════════════════════════════════════════════════


class TestContradiction:
    def test_fires_at_the_constitutional_count_of_three(self):
        v = decide(_stats(contradictions=3, supports=1), now=NOW)
        assert v.verdict == cfg.VERDICT_CONTRADICT
        assert v.acts

    def test_two_contradictions_is_not_enough(self):
        v = decide(_stats(contradictions=2, supports=8), now=NOW)
        assert v.verdict == cfg.VERDICT_KEEP

    def test_execution_failures_are_not_counter_evidence(self):
        """The single most important rule: absence of a result is not a result.

        30 failed decisions and zero real outcomes must never retire a lesson,
        or one infrastructure outage prunes the whole corpus.
        """
        v = decide(_stats(contradictions=0, supports=0, non_evidence=30), now=NOW)
        assert v.verdict == cfg.VERDICT_SKIP_UNTESTED
        assert v.evidence["measured"] == 0

    def test_non_evidence_is_excluded_from_the_rate_denominator(self):
        """Counting failures as support would be the mirror error — it would
        dilute a real 100% contradiction rate to 9% and make the rule
        unreachable."""
        s = _stats(contradictions=3, supports=0, non_evidence=30)
        assert s.measured == 3
        assert s.contradiction_rate_pct == 100.0
        assert decide(s, now=NOW).verdict == cfg.VERDICT_CONTRADICT

    def test_emergency_cliff_fires_on_overwhelming_disagreement(self):
        v = decide(_stats(contradictions=18, supports=4), now=NOW)
        assert v.verdict == cfg.VERDICT_CONTRADICT
        assert "emergency cliff" in v.reason

    def test_emergency_cliff_needs_the_sample_not_just_the_rate(self):
        """100% of 3 is not an emergency — it falls through to the ordinary
        count rule, which is the intended, gentler path."""
        v = decide(_stats(contradictions=3, supports=0), now=NOW)
        assert v.verdict == cfg.VERDICT_CONTRADICT
        assert "emergency cliff" not in v.reason

    def test_rate_guard_spares_a_high_volume_lesson_when_enabled(self, monkeypatch):
        """3-of-300 is a great lesson. The constitutions fix the count at 3, so
        the rate guard is off by default; enabling it must only ever spare."""
        monkeypatch.setattr(svc, "CONTRADICTION_MIN_RATE_PCT", 40.0)
        v = decide(_stats(contradictions=3, supports=297), now=NOW)
        assert v.verdict == cfg.VERDICT_KEEP
        assert "below" in v.reason

    def test_rate_guard_off_by_default_matches_the_constitution(self):
        assert cfg.CONTRADICTION_MIN_RATE_PCT == 0.0
        assert decide(_stats(contradictions=3, supports=297), now=NOW).verdict == (
            cfg.VERDICT_CONTRADICT
        )


class TestSafetyExclusions:
    def test_anti_playbook_is_never_contradicted_however_bad_the_evidence(self):
        """The most expensive false positive available: retiring a warning
        makes the fleet resume what it learned not to do."""
        v = decide(_stats(entry_kind="anti_playbook", contradictions=50, supports=0), now=NOW)
        assert v.verdict == cfg.VERDICT_SKIP_EXCLUDED_KIND
        assert not v.acts

    def test_unmeasurable_domain_is_never_acted_on(self):
        for domain in ("vera", "cora", "hunter", "fleet"):
            v = decide(_stats(agent_domain=domain, contradictions=99), now=NOW)
            assert v.verdict == cfg.VERDICT_SKIP_UNMEASURABLE, domain
            assert not v.acts

    def test_unmeasurable_domain_is_not_flagged_stale_either(self):
        """The dangerous half. With no feed, 'no supporting evidence' is
        permanently true, so an age rule would flag 100% of these forever."""
        v = decide(
            _stats(
                agent_domain="vera",
                supports=0,
                last_evidence_at=None,
                authored_at=NOW - timedelta(days=900),
            ),
            now=NOW,
        )
        assert v.verdict == cfg.VERDICT_SKIP_UNMEASURABLE

    def test_orphaned_source_is_reported_not_retired(self):
        v = decide(_stats(source_resolved=False, contradictions=10), now=NOW)
        assert v.verdict == cfg.VERDICT_SKIP_ORPHANED_SOURCE
        assert not v.acts

    def test_already_terminal_statuses_are_left_alone(self):
        for status in ("rejected", "retired", "superseded", "contradicted"):
            v = decide(_stats(status=status, contradictions=99), now=NOW)
            assert v.verdict == cfg.VERDICT_SKIP_NOT_ACTIONABLE, status

    def test_untested_lesson_is_not_judged(self):
        v = decide(_stats(contradictions=1, supports=0), now=NOW)
        assert v.verdict == cfg.VERDICT_SKIP_UNTESTED


class TestStaleness:
    def test_stale_lesson_is_reported_never_mutated(self):
        v = decide(
            _stats(supports=5, last_evidence_at=NOW - timedelta(days=90)),
            now=NOW,
        )
        assert v.verdict == cfg.VERDICT_REPORT_STALE
        assert not v.acts
        assert v.evidence["age_days"] == 90

    def test_stable_winner_is_not_retired_for_being_old(self):
        """A lesson right for months looks identical to a dead one under a
        row-age rule. Recent evidence must protect it."""
        v = decide(
            _stats(
                authored_at=NOW - timedelta(days=400),
                supports=200,
                last_evidence_at=NOW - timedelta(days=2),
            ),
            now=NOW,
        )
        assert v.verdict == cfg.VERDICT_KEEP

    def test_never_exercised_lesson_ages_from_authored_at(self):
        v = decide(
            _stats(
                supports=0, contradictions=0, last_evidence_at=None,
                authored_at=NOW - timedelta(days=200),
            ),
            now=NOW,
        )
        assert v.verdict == cfg.VERDICT_SKIP_UNTESTED

    def test_self_healing_kill_uses_its_own_shorter_clock(self):
        v = decide(
            _stats(
                source_type="self_healing_kill", source_id="some_metric",
                supports=5, last_evidence_at=NOW - timedelta(days=35),
            ),
            now=NOW,
        )
        assert v.verdict == cfg.VERDICT_REPORT_STALE
        assert v.evidence["staleness_limit_days"] == 30


class TestOrdering:
    def test_contradiction_beats_staleness(self):
        """Both apply. Superseding first would attach the stronger evidence to
        an already-retired row and lose it."""
        v = decide(
            _stats(contradictions=3, supports=1, last_evidence_at=NOW - timedelta(days=300)),
            now=NOW,
        )
        assert v.verdict == cfg.VERDICT_CONTRADICT

    def test_contradiction_beats_supersession(self):
        v = decide(_stats(contradictions=3, supports=1, successor_id=99), now=NOW)
        assert v.verdict == cfg.VERDICT_CONTRADICT

    def test_supersession_beats_staleness(self):
        v = decide(
            _stats(supports=5, successor_id=99, last_evidence_at=NOW - timedelta(days=300)),
            now=NOW,
        )
        assert v.verdict == cfg.VERDICT_SUPERSEDE
        assert v.evidence["successor_id"] == 99


class TestConfig:
    def test_config_is_internally_consistent(self):
        assert cfg.validate_hygiene_config() == []

    def test_signal_vocabularies_do_not_overlap(self):
        for sets in (
            (cfg.CONTRADICTION_TERMINAL_STATUSES, cfg.SUPPORT_TERMINAL_STATUSES,
             cfg.NON_EVIDENCE_TERMINAL_STATUSES),
            (cfg.CONTRADICTION_AUTONOMY_CLASSES, cfg.SUPPORT_AUTONOMY_CLASSES,
             cfg.NON_EVIDENCE_AUTONOMY_CLASSES),
        ):
            a, b, c = sets
            assert not (a & b) and not (a & c) and not (b & c)

    def test_blast_radius_has_an_absolute_floor(self):
        """5% of 7 rows is 0 — a pure percentage would make the job incapable
        of ever acting at the corpus size that actually exists."""
        assert cfg.blast_radius_cap(7) == cfg.BLAST_RADIUS_MIN
        assert cfg.blast_radius_cap(0) == 0
        assert cfg.blast_radius_cap(1000) == 200

    def test_every_verdict_has_a_digest_label(self):
        """An unlabelled verdict would print as a bare slug in the daily
        digest, which is how a named state becomes invisible."""
        assert cfg.ALL_VERDICTS <= set(_VERDICT_LABELS)

    def test_thresholds_are_frozen_into_the_snapshot(self):
        snap = cfg.config_snapshot()
        assert snap["contradiction_min_count"] == cfg.CONTRADICTION_MIN_COUNT
        assert snap["excluded_kinds"] == list(cfg.HYGIENE_EXCLUDED_KINDS)

    def test_constitutional_count_is_three(self):
        """Guard rather than a tautology: if someone tunes this, they should
        have to delete a test that says why they cannot."""
        assert cfg.CONTRADICTION_MIN_COUNT == 3


# ══════════════════════════════════════════════════════════════════════════
# Tier 2 — real Postgres
# ══════════════════════════════════════════════════════════════════════════


_LEARN_DDL = [
    "ALTER TABLE lifecycle_playbook ADD COLUMN IF NOT EXISTS confidence INTEGER",
    "ALTER TABLE lifecycle_playbook ADD COLUMN IF NOT EXISTS scope JSONB",
    "ALTER TABLE lifecycle_playbook ADD COLUMN IF NOT EXISTS version INTEGER NOT NULL DEFAULT 1",
    "ALTER TABLE lifecycle_playbook ADD COLUMN IF NOT EXISTS superseded_by_id BIGINT "
    "REFERENCES lifecycle_playbook(id) ON DELETE SET NULL",
    """
    DO $$
    BEGIN
        IF EXISTS (
            SELECT 1 FROM information_schema.table_constraints
            WHERE table_name = 'lifecycle_playbook'
              AND constraint_name = 'check_lifecycle_playbook_status'
        ) THEN
            ALTER TABLE lifecycle_playbook DROP CONSTRAINT check_lifecycle_playbook_status;
        END IF;
        ALTER TABLE lifecycle_playbook ADD CONSTRAINT check_lifecycle_playbook_status
            CHECK (status IN ('recommended','adopted','rejected','retired',
                              'superseded','contradicted'));
    END $$;
    """,
]


@pytest.fixture
def learn_db(fresh_db):
    """fresh_db with the LEARN Layer 4 DDL applied inside the test transaction."""
    for stmt in _LEARN_DDL:
        fresh_db.execute(sa_text(stmt))
    fresh_db.flush()
    return fresh_db


def _insert_lesson(
    db,
    *,
    name: str,
    status: str = "adopted",
    entry_kind: str = "playbook",
    agent_domain: str = "lifecycle",
    source_type: str = "ab_test",
    source_id: str | None = None,
    authored_days_ago: int = 10,
    scope: str | None = None,
) -> int:
    """Insert one lesson, returning its id. Raw SQL — repo convention."""
    sid = source_id if source_id is not None else f"src_{uuid.uuid4().hex[:8]}"
    row = db.execute(sa_text("""
        INSERT INTO lifecycle_playbook (
            name, description, pattern_json, authored_by, authored_at, status,
            source_type, source_id, source_key, agent_domain, entry_kind,
            version, scope, created_at, updated_at
        ) VALUES (
            :name, 'test lesson', '{}'::jsonb, 'lifecycle',
            now() - (:days * interval '1 day'), :status,
            :source_type, :source_id, :source_key, :agent_domain, :entry_kind,
            1, CAST(:scope AS jsonb), now(), now()
        ) RETURNING id
    """), {
        "name": name,
        "days": authored_days_ago,
        "status": status,
        "source_type": source_type,
        "source_id": sid,
        "source_key": f"{source_type}:{sid}:{uuid.uuid4().hex[:6]}",
        "agent_domain": agent_domain,
        "entry_kind": entry_kind,
        "scope": scope,
    }).first()
    return int(row.id)


def _insert_ab_test(db, test_name: str) -> None:
    db.execute(sa_text("""
        INSERT INTO ab_tests (test_name, segment, variant_a, variant_b,
                              traffic_pct, status, started_at)
        VALUES (:name, 'test', '{}'::jsonb, '{}'::jsonb, 10, 'active', now())
        ON CONFLICT (test_name) DO NOTHING
    """), {"name": test_name})


def _insert_decision(
    db,
    *,
    playbook_id: int | None,
    terminal_status: str = "completed",
    autonomy_class: str = "autonomous",
    overridden: bool = False,
    days_ago: int = 1,
    graph_name: str = "lifecycle_retention",
) -> str:
    did = str(uuid.uuid4())
    db.execute(sa_text("""
        INSERT INTO agent_decisions (
            decision_id, graph_name, started_at, completed_at, terminal_status,
            tokens_used, cost_usd, autonomy_class, was_autonomous,
            requires_approval, overridden_at, playbook_id
        ) VALUES (
            :did, :graph, now() - (:days * interval '1 day'), now(), :ts,
            0, 0, :ac, true, false,
            CASE WHEN :ovr THEN now() ELSE NULL END, :pid
        )
    """), {
        "did": did, "graph": graph_name, "days": days_ago,
        "ts": terminal_status, "ac": autonomy_class, "ovr": overridden,
        "pid": playbook_id,
    })
    return did


def _find(stats_list, lesson_id):
    return next(s for s in stats_list if s.lesson_id == lesson_id)


class TestSchemaRail:
    def test_reports_not_ready_when_the_migration_has_not_run(self, fresh_db):
        """The live shared DB is exactly this case: the CHECK permits four
        values and 'contradicted' is not one, so mark_contradicted would raise.
        The rail turns that crash into a reported refusal."""
        readiness = svc.check_schema_readiness(fresh_db)
        if readiness.ready:
            pytest.skip("environment already migrated — rail covered by the unit path")

        report = svc.sweep(fresh_db, now=NOW, dry_run=False)
        assert report.run_status == cfg.RUN_SCHEMA_NOT_READY
        assert report.actions == []

    def test_reports_ready_once_the_ddl_is_applied(self, learn_db):
        readiness = svc.check_schema_readiness(learn_db)
        assert readiness.ready
        assert readiness.missing_columns == ()
        assert readiness.missing_status_values == ()

    def test_classification_covers_the_live_check_constraints(self, fresh_db):
        """ADR 0006's lesson in its most useful form: a value added to either
        CHECK later must not silently fall through as unclassified and be
        quietly dropped from the evidence."""
        for constraint, sets in (
            ("check_agent_terminal_status", (
                cfg.CONTRADICTION_TERMINAL_STATUSES | cfg.SUPPORT_TERMINAL_STATUSES
                | cfg.NON_EVIDENCE_TERMINAL_STATUSES
            )),
            ("check_agent_autonomy_class", (
                cfg.CONTRADICTION_AUTONOMY_CLASSES | cfg.SUPPORT_AUTONOMY_CLASSES
                | cfg.NON_EVIDENCE_AUTONOMY_CLASSES
            )),
        ):
            cdef = fresh_db.execute(sa_text("""
                SELECT pg_get_constraintdef(oid) FROM pg_constraint
                WHERE conrelid = 'agent_decisions'::regclass AND conname = :n
            """), {"n": constraint}).scalar()
            if not cdef:
                pytest.skip(f"{constraint} not present")
            permitted = set(__import__("re").findall(r"'([a-z_]+)'::character varying", cdef))
            unclassified = permitted - sets
            assert not unclassified, (
                f"{constraint} permits {sorted(unclassified)} which config "
                f"classifies as neither contradiction, support, nor non-evidence"
            )


class TestFeedHealth:
    def test_no_evidence_yet_is_distinct_from_a_stale_feed(self, learn_db):
        learn_db.execute(sa_text(
            "DELETE FROM agent_decisions WHERE playbook_id IS NOT NULL"
        ))
        learn_db.flush()
        assert svc.check_feed_health(learn_db, now=NOW).state == "no_evidence_yet"

    def test_feed_that_went_quiet_is_flagged_stale(self, learn_db):
        learn_db.execute(sa_text(
            "DELETE FROM agent_decisions WHERE playbook_id IS NOT NULL"
        ))
        lesson = _insert_lesson(learn_db, name="ab_winner:quiet")
        _insert_decision(learn_db, playbook_id=lesson, days_ago=60)
        learn_db.flush()

        health = svc.check_feed_health(learn_db, now=svc._utcnow())
        assert health.state == "feed_stale"
        assert health.linked_rows == 1

    def test_own_audit_rows_do_not_make_the_feed_look_healthy(self, learn_db):
        learn_db.execute(sa_text(
            "DELETE FROM agent_decisions WHERE playbook_id IS NOT NULL"
        ))
        lesson = _insert_lesson(learn_db, name="ab_winner:selfonly")
        _insert_decision(
            learn_db, playbook_id=lesson, graph_name=cfg.HYGIENE_GRAPH_NAME,
        )
        learn_db.flush()
        assert svc.check_feed_health(learn_db, now=svc._utcnow()).state == "no_evidence_yet"


class TestAggregateQuery:
    def test_own_audit_rows_are_not_counted_as_supporting_evidence(self, learn_db):
        """Self-poisoning guard. agent_decisions.playbook_id is both the
        evidence feed and the audit sink, so without the graph_name filter
        every run would manufacture one support row for the lesson it judged."""
        lesson = _insert_lesson(learn_db, name="ab_winner:selfpoison")
        for _ in range(5):
            _insert_decision(
                learn_db, playbook_id=lesson, graph_name=cfg.HYGIENE_GRAPH_NAME,
            )
        learn_db.flush()

        stats = _find(svc.load_lesson_stats(learn_db), lesson)
        assert stats.supports == 0
        assert stats.measured == 0

    def test_overridden_decisions_count_as_contradictions(self, learn_db):
        lesson = _insert_lesson(learn_db, name="ab_winner:overridden")
        for _ in range(3):
            _insert_decision(
                learn_db, playbook_id=lesson, terminal_status="completed",
                autonomy_class="overridden", overridden=True,
            )
        _insert_decision(learn_db, playbook_id=lesson)
        learn_db.flush()

        stats = _find(svc.load_lesson_stats(learn_db), lesson)
        assert stats.contradictions == 3
        assert stats.supports == 1

    def test_failed_decisions_land_in_non_evidence(self, learn_db):
        lesson = _insert_lesson(learn_db, name="ab_winner:failures")
        for _ in range(4):
            _insert_decision(
                learn_db, playbook_id=lesson, terminal_status="failed",
                autonomy_class="autonomous",
            )
        learn_db.flush()

        stats = _find(svc.load_lesson_stats(learn_db), lesson)
        assert stats.non_evidence == 4
        assert stats.contradictions == 0
        assert stats.measured == 0

    def test_non_evidence_outranks_contradiction(self, learn_db):
        """A decision that never produced an outcome must not be held against
        the lesson on some other dimension. This is the rail that stops an
        outage from retiring the corpus: 'failed' wins over 'rejected'."""
        lesson = _insert_lesson(learn_db, name="ab_winner:precedence")
        for _ in range(5):
            _insert_decision(
                learn_db, playbook_id=lesson, terminal_status="failed",
                autonomy_class="rejected",
            )
        learn_db.flush()

        stats = _find(svc.load_lesson_stats(learn_db), lesson)
        assert stats.non_evidence == 5
        assert stats.contradictions == 0
        assert stats.measured == 0

    def test_explicit_human_reversal_outranks_everything(self, learn_db):
        lesson = _insert_lesson(learn_db, name="ab_winner:reversed")
        for _ in range(3):
            _insert_decision(
                learn_db, playbook_id=lesson, terminal_status="failed",
                autonomy_class="approval_required", overridden=True,
            )
        learn_db.flush()

        stats = _find(svc.load_lesson_stats(learn_db), lesson)
        assert stats.contradictions == 3
        assert stats.non_evidence == 0

    def test_each_decision_is_counted_exactly_once(self, learn_db):
        lesson = _insert_lesson(learn_db, name="ab_winner:partition")
        _insert_decision(learn_db, playbook_id=lesson, terminal_status="completed")
        _insert_decision(
            learn_db, playbook_id=lesson, terminal_status="aborted",
            autonomy_class="rejected",
        )
        _insert_decision(learn_db, playbook_id=lesson, terminal_status="failed")
        learn_db.flush()

        stats = _find(svc.load_lesson_stats(learn_db), lesson)
        assert stats.supports + stats.contradictions + stats.non_evidence == 3

    def test_contradictions_do_not_reset_the_freshness_clock(self, learn_db):
        """A lesson failing repeatedly is not thereby fresh."""
        lesson = _insert_lesson(learn_db, name="ab_winner:failingfresh")
        for _ in range(3):
            _insert_decision(
                learn_db, playbook_id=lesson, terminal_status="aborted",
                autonomy_class="rejected", days_ago=1,
            )
        learn_db.flush()
        assert _find(svc.load_lesson_stats(learn_db), lesson).last_evidence_at is None

    def test_evidence_outside_the_window_is_not_counted(self, learn_db):
        lesson = _insert_lesson(learn_db, name="ab_winner:oldevidence")
        for _ in range(5):
            _insert_decision(
                learn_db, playbook_id=lesson, terminal_status="aborted",
                autonomy_class="rejected",
                days_ago=cfg.CONTRADICTION_WINDOW_DAYS + 10,
            )
        learn_db.flush()

        stats = _find(svc.load_lesson_stats(learn_db), lesson)
        assert stats.contradictions == 0

    def test_dangling_source_id_is_detected_as_orphaned(self, learn_db):
        lesson = _insert_lesson(
            learn_db, name="ab_winner:test_rollback_dead",
            source_id="test_rollback_does_not_exist",
        )
        learn_db.flush()
        assert _find(svc.load_lesson_stats(learn_db), lesson).source_resolved is False

    def test_resolvable_source_id_is_not_orphaned(self, learn_db):
        _insert_ab_test(learn_db, "hygiene_real_test")
        lesson = _insert_lesson(
            learn_db, name="ab_winner:real", source_id="hygiene_real_test",
        )
        learn_db.flush()
        assert _find(svc.load_lesson_stats(learn_db), lesson).source_resolved is True

    def test_unknown_source_type_is_not_treated_as_orphaned(self, learn_db):
        """self_healing_kill's source_id is a metric name, not a row. Defaulting
        unresolvable-to-check to 'orphaned' would flag all of them."""
        lesson = _insert_lesson(
            learn_db, name="kill:some_metric", source_type="self_healing_kill",
            source_id="some_metric",
        )
        learn_db.flush()
        assert _find(svc.load_lesson_stats(learn_db), lesson).source_resolved is True


class TestSuccessorDetection:
    def test_newer_lesson_with_the_same_scope_is_a_successor(self, learn_db):
        scope = '{"buyer_type": "buy_and_hold", "offer": "founder_tier"}'
        old = _insert_lesson(learn_db, name="old", authored_days_ago=30, scope=scope)
        new = _insert_lesson(learn_db, name="new", authored_days_ago=1, scope=scope)
        learn_db.flush()
        assert _find(svc.load_lesson_stats(learn_db), old).successor_id == new

    def test_null_scope_never_matches(self, learn_db):
        """Two lessons with no declared subject are not the same subject.
        Matching them would supersede unrelated lessons against each other."""
        old = _insert_lesson(learn_db, name="null_old", authored_days_ago=30)
        _insert_lesson(learn_db, name="null_new", authored_days_ago=1)
        learn_db.flush()
        assert _find(svc.load_lesson_stats(learn_db), old).successor_id is None

    def test_different_scope_is_not_a_successor(self, learn_db):
        old = _insert_lesson(
            learn_db, name="scoped_a", authored_days_ago=30,
            scope='{"offer": "founder_tier"}',
        )
        _insert_lesson(
            learn_db, name="scoped_b", authored_days_ago=1,
            scope='{"offer": "standard"}',
        )
        learn_db.flush()
        assert _find(svc.load_lesson_stats(learn_db), old).successor_id is None

    def test_supersession_cannot_form_a_two_cycle(self, learn_db):
        """A superseded by B must not let B be superseded by A."""
        scope = '{"offer": "cycle_test"}'
        a = _insert_lesson(learn_db, name="cyc_a", authored_days_ago=30, scope=scope)
        b = _insert_lesson(learn_db, name="cyc_b", authored_days_ago=1, scope=scope)
        learn_db.flush()

        assert svc.supersede_recommendation(learn_db, a, b) is True
        learn_db.flush()

        # B is now newer and actionable; A points at B and is 'superseded'.
        stats = svc.load_lesson_stats(learn_db)
        assert _find(stats, b).successor_id != a
        assert _find(stats, a).status == "superseded"

    def test_an_anti_playbook_is_not_a_successor_to_a_playbook(self, learn_db):
        scope = '{"offer": "kind_test"}'
        old = _insert_lesson(learn_db, name="k_old", authored_days_ago=30, scope=scope)
        _insert_lesson(
            learn_db, name="k_new", authored_days_ago=1, scope=scope,
            entry_kind="anti_playbook",
        )
        learn_db.flush()
        assert _find(svc.load_lesson_stats(learn_db), old).successor_id is None


class TestSweep:
    def _contradicted_lesson(self, db, name="ab_winner:doomed"):
        _insert_ab_test(db, "hygiene_doomed")
        lesson = _insert_lesson(db, name=name, source_id="hygiene_doomed")
        for _ in range(3):
            _insert_decision(
                db, playbook_id=lesson, terminal_status="aborted",
                autonomy_class="rejected",
            )
        _insert_decision(db, playbook_id=lesson)
        db.flush()
        return lesson

    def test_dry_run_changes_nothing(self, learn_db):
        lesson = self._contradicted_lesson(learn_db)
        report = svc.sweep(learn_db, dry_run=True)

        assert report.run_status == cfg.RUN_OK
        mine = [a for a in report.actions if a["lesson_id"] == lesson]
        assert mine and mine[0]["verdict"] == cfg.VERDICT_CONTRADICT
        assert mine[0]["applied"] is False
        assert mine[0]["skipped_reason"] == "dry_run"

        status = learn_db.execute(sa_text(
            "SELECT status FROM lifecycle_playbook WHERE id = :id"
        ), {"id": lesson}).scalar()
        assert status == "adopted"

    def test_apply_marks_contradicted_and_writes_an_audit_row(self, learn_db):
        lesson = self._contradicted_lesson(learn_db)
        report = svc.sweep(learn_db, dry_run=False)

        mine = [a for a in report.actions if a["lesson_id"] == lesson]
        assert mine and mine[0]["applied"] is True

        status = learn_db.execute(sa_text(
            "SELECT status FROM lifecycle_playbook WHERE id = :id"
        ), {"id": lesson}).scalar()
        assert status == "contradicted"

        audit = learn_db.execute(sa_text("""
            SELECT summary FROM agent_decisions
            WHERE playbook_id = :id AND graph_name = :graph
        """), {"id": lesson, "graph": cfg.HYGIENE_GRAPH_NAME}).first()
        assert audit is not None
        assert audit.summary["verdict"] == cfg.VERDICT_CONTRADICT
        assert audit.summary["evidence"]["contradictions"] == 3
        assert audit.summary["thresholds"]["contradiction_min_count"] == 3

    def test_one_lesson_erroring_does_not_abort_or_taint_another(self, learn_db, monkeypatch):
        """A savepoint per lesson means one failure must not roll back a
        sibling's successful mutation in the same run, and must not leave the
        failed lesson half-changed."""
        good = self._contradicted_lesson(learn_db, name="ab_winner:good")
        bad = self._contradicted_lesson(learn_db, name="ab_winner:bad")

        real_mark_contradicted = svc.mark_contradicted

        def _boom(session, playbook_id):
            if playbook_id == bad:
                raise RuntimeError("simulated failure")
            return real_mark_contradicted(session, playbook_id)

        monkeypatch.setattr(svc, "mark_contradicted", _boom)

        report = svc.sweep(learn_db, dry_run=False)

        good_action = next(a for a in report.actions if a["lesson_id"] == good)
        bad_action = next(a for a in report.actions if a["lesson_id"] == bad)
        assert good_action["applied"] is True
        assert bad_action["applied"] is False
        assert "simulated failure" in bad_action["error"]

        statuses = dict(learn_db.execute(sa_text(
            "SELECT id, status FROM lifecycle_playbook WHERE id = ANY(:ids)"
        ), {"ids": [good, bad]}).all())
        assert statuses[good] == "contradicted"
        assert statuses[bad] == "adopted"

        # The failed lesson must not have a stray audit row from a
        # half-completed savepoint — ROLLBACK TO SAVEPOINT undoes the
        # INSERT along with the tool's UPDATE.
        bad_audit = learn_db.execute(sa_text("""
            SELECT count(*) FROM agent_decisions
            WHERE playbook_id = :id AND graph_name = :graph
        """), {"id": bad, "graph": cfg.HYGIENE_GRAPH_NAME}).scalar()
        assert bad_audit == 0

    def test_rerunning_is_idempotent(self, learn_db):
        lesson = self._contradicted_lesson(learn_db)
        svc.sweep(learn_db, dry_run=False)
        second = svc.sweep(learn_db, dry_run=False)

        # Now 'contradicted', so the tools refuse it and decide() skips it.
        mine = [a for a in second.actions if a["lesson_id"] == lesson]
        assert mine == []
        status = learn_db.execute(sa_text(
            "SELECT status FROM lifecycle_playbook WHERE id = :id"
        ), {"id": lesson}).scalar()
        assert status == "contradicted"

    def test_supersede_path_end_to_end(self, learn_db):
        scope = '{"offer": "sweep_supersede"}'
        _insert_ab_test(learn_db, "hygiene_sup_old")
        old = _insert_lesson(
            learn_db, name="sup_old", source_id="hygiene_sup_old",
            authored_days_ago=30, scope=scope,
        )
        _insert_ab_test(learn_db, "hygiene_sup_new")
        new = _insert_lesson(
            learn_db, name="sup_new", source_id="hygiene_sup_new",
            authored_days_ago=1, scope=scope,
        )
        for _ in range(3):
            _insert_decision(learn_db, playbook_id=old)
        learn_db.flush()

        report = svc.sweep(learn_db, dry_run=False)
        mine = [a for a in report.actions if a["lesson_id"] == old]
        assert mine and mine[0]["verdict"] == cfg.VERDICT_SUPERSEDE
        assert mine[0]["applied"] is True

        row = learn_db.execute(sa_text(
            "SELECT status, superseded_by_id FROM lifecycle_playbook WHERE id = :id"
        ), {"id": old}).first()
        assert row.status == "superseded"
        assert row.superseded_by_id == new

    def test_blast_radius_refusal_mutates_nothing(self, learn_db):
        """A run that wants to retire most of the measurable corpus is a bug
        report, not a result.

        This is a shared dev DB other work touches concurrently, so the test
        computes its own margin from a measured baseline rather than assuming
        today's ambient row count — inserting comfortably more than
        20% of (baseline + inserted) would ever allow through, at any
        plausible ambient scale.
        """
        baseline = svc.sweep(learn_db, dry_run=True)
        baseline_measurable = baseline.measurable_population

        n = max(cfg.BLAST_RADIUS_MIN + 2, int(baseline_measurable * 0.5) + 10)
        suffix = uuid.uuid4().hex[:8]
        lessons = []
        for i in range(n):
            test_name = f"hygiene_blast_{suffix}_{i}"
            _insert_ab_test(learn_db, test_name)
            lesson = _insert_lesson(
                learn_db, name=f"blast_{suffix}_{i}", source_id=test_name,
            )
            for _ in range(3):
                _insert_decision(
                    learn_db, playbook_id=lesson, terminal_status="aborted",
                    autonomy_class="rejected",
                )
            _insert_decision(learn_db, playbook_id=lesson)
            lessons.append(lesson)
        learn_db.flush()

        report = svc.sweep(learn_db, dry_run=False)
        assert report.run_status == cfg.RUN_BLAST_RADIUS_EXCEEDED
        assert report.actions == []

        statuses = learn_db.execute(sa_text("""
            SELECT DISTINCT status FROM lifecycle_playbook WHERE id = ANY(:ids)
        """), {"ids": lessons}).scalars().all()
        assert statuses == ["adopted"]

    def test_limit_caps_mutations_and_says_so(self, learn_db):
        lesson = self._contradicted_lesson(learn_db)
        report = svc.sweep(learn_db, dry_run=False, limit=0)
        assert [a for a in report.actions if a["lesson_id"] == lesson] == []
        assert any("not attempted" in n for n in report.notes)

    def test_orphaned_lessons_do_not_inflate_the_blast_radius_budget(self, learn_db):
        """Orphaned lessons (the 7 test-pollution rows in the shared DB among
        them) must never enlarge the mutation budget for lessons that do have
        evidence.

        Asserted as a delta against a measured baseline, not an absolute
        count — this is a shared dev DB other work touches concurrently, and
        the invariant under test is "orphaned inserts don't move the
        measurable count", which a baseline delta proves regardless of
        whatever else is in the corpus today.
        """
        baseline = svc.sweep(learn_db, dry_run=True)
        baseline_measurable = baseline.measurable_population
        baseline_orphaned = baseline.counts.get(cfg.VERDICT_SKIP_ORPHANED_SOURCE, 0)

        for _ in range(20):
            suffix = uuid.uuid4().hex[:8]
            _insert_lesson(
                learn_db, name=f"orph_{suffix}", source_id=f"orphan_missing_{suffix}",
            )
        learn_db.flush()

        report = svc.sweep(learn_db, dry_run=True)
        assert report.counts.get(cfg.VERDICT_SKIP_ORPHANED_SOURCE, 0) >= baseline_orphaned + 20
        assert report.measurable_population == baseline_measurable


class TestDigest:
    def test_unmeasurable_counts_lead_the_digest(self):
        report = svc.HygieneReport(
            run_status=cfg.RUN_OK,
            dry_run=True,
            schema=svc.SchemaReadiness(ready=True),
            feed=svc.FeedHealth("no_evidence_yet", 0, None),
            counts={cfg.VERDICT_SKIP_UNMEASURABLE: 412, cfg.VERDICT_KEEP: 3},
            measurable_population=3,
            cap=3,
        )
        digest = format_digest(report)
        assert "Not covered by this sweep" in digest
        assert "412 unmeasurable" in digest
        assert digest.index("412 unmeasurable") < digest.index("Verdicts")

    def test_a_failed_action_is_never_dropped_by_a_successful_one(self):
        """Regression: grouping solely on truthy 'applied' put any successes
        in *Mutated* and, since the code took an if/elif, silently dropped a
        same-run failure from the digest text entirely — visible only in
        server logs. One success must never hide another lesson's failure."""
        report = svc.HygieneReport(
            run_status=cfg.RUN_OK,
            dry_run=False,
            schema=svc.SchemaReadiness(ready=True),
            feed=svc.FeedHealth("ok", 10, NOW),
            actions=[
                {
                    "lesson_id": 1, "lesson_name": "ok_one",
                    "verdict": cfg.VERDICT_CONTRADICT, "reason": "3 contradictions",
                    "applied": True,
                },
                {
                    "lesson_id": 2, "lesson_name": "broken_one",
                    "verdict": cfg.VERDICT_CONTRADICT, "reason": "3 contradictions",
                    "applied": False, "error": "connection reset",
                },
            ],
        )
        digest = format_digest(report)
        assert "broken_one" in digest
        assert "Failed to apply" in digest
        assert "ok_one" in digest
        assert "Mutated" in digest

    def test_dry_run_planned_actions_do_not_hide_behind_a_mutated_entry(self):
        report = svc.HygieneReport(
            run_status=cfg.RUN_OK,
            dry_run=True,
            schema=svc.SchemaReadiness(ready=True),
            feed=svc.FeedHealth("ok", 10, NOW),
            actions=[{
                "lesson_id": 3, "lesson_name": "would_do_this",
                "verdict": cfg.VERDICT_SUPERSEDE, "reason": "newer scope match",
                "applied": False, "skipped_reason": "dry_run",
            }],
        )
        digest = format_digest(report)
        assert "would_do_this" in digest
        assert "Would mutate" in digest

    def test_schema_refusal_names_the_migration(self):
        report = svc.HygieneReport(
            run_status=cfg.RUN_SCHEMA_NOT_READY,
            dry_run=False,
            schema=svc.SchemaReadiness(
                ready=False, missing_columns=("confidence",),
                missing_status_values=("contradicted",),
            ),
            feed=svc.FeedHealth("no_evidence_yet", 0, None),
        )
        digest = format_digest(report)
        assert "apply_lifecycle_playbook_lessons_versioning.py" in digest
        assert "Nothing was touched" in digest


class TestDigestPosting:
    """Regression tests for two bugs an audit pass caught before push:

    1. _post_digest read settings.county_launch_slack_channel — a
       copy-paste leftover from mirroring county_launch_evaluator.py's
       pattern. Posting a lesson-hygiene digest into the county-launch
       approval channel is a wrong-audience bug, not a cosmetic one.
    2. run() only posted to Slack when report.actions was non-empty or the
       run_status was alerting. On the actual shared DB today (all 7 lessons
       orphaned, nothing acting, RUN_OK), that condition is never met, so the
       "not covered by this sweep" counts — the ones this job's whole
       argument says must surface or the follow-up ticket never gets filed —
       were silently buried in a log line instead of reaching the channel.
    """

    def test_digest_channel_is_its_own_setting_not_county_launch(self, monkeypatch):
        settings = get_settings()
        monkeypatch.setattr(settings, "learning_hygiene_slack_channel", "#lesson-hygiene")
        monkeypatch.setattr(settings, "county_launch_slack_channel", "#county-launch")

        from unittest.mock import MagicMock

        fake_client = MagicMock()
        fake_module = MagicMock()
        fake_module.WebClient.return_value = fake_client
        monkeypatch.setitem(__import__("sys").modules, "slack_sdk", fake_module)

        from pydantic import SecretStr
        monkeypatch.setattr(settings, "slack_bot_token", SecretStr("xoxb-test"))

        task_mod._post_digest("hello")

        fake_client.chat_postMessage.assert_called_once()
        assert fake_client.chat_postMessage.call_args.kwargs["channel"] == "#lesson-hygiene"

    def test_no_channel_configured_logs_only_and_never_raises(self, monkeypatch):
        settings = get_settings()
        monkeypatch.setattr(settings, "learning_hygiene_slack_channel", "")
        task_mod._post_digest("hello")  # must not raise

    def test_run_posts_even_when_nothing_acted_and_status_is_ok(self, monkeypatch, learn_db):
        """The exact bug: RUN_OK + empty actions must still post, because
        that is the state the live shared DB is in today."""
        calls = []
        monkeypatch.setattr(task_mod, "_post_digest", lambda digest: calls.append(digest))
        monkeypatch.setattr(
            task_mod, "get_db_context",
            lambda: __import__("contextlib").nullcontext(learn_db),
        )

        result = run(dry_run=True, post_digest=True)

        assert result["run_status"] == cfg.RUN_OK
        assert calls, "digest must post even when run_status is OK and nothing acted"

    def test_run_does_not_post_when_post_digest_false(self, monkeypatch, learn_db):
        calls = []
        monkeypatch.setattr(task_mod, "_post_digest", lambda digest: calls.append(digest))
        monkeypatch.setattr(
            task_mod, "get_db_context",
            lambda: __import__("contextlib").nullcontext(learn_db),
        )

        run(dry_run=True, post_digest=False)
        assert not calls
