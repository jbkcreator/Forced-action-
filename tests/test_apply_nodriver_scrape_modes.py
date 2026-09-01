"""
migrations/apply_nodriver_scrape_modes.py — the pinellas/liens
scrape_mode migration.

Regression coverage for a real bug found in PR review: the migration's
docstring claims it's "safe to re-run" and "idempotent (guarded by a WHERE
that only fires when scrape_mode is still the pre-migration value)", but the
actual condition was inverted — it skipped only when the mode was ALREADY
the post-migration value (nodriver_then_ai) and overwrote for every other
mode, including one an operator deliberately changed afterward (a
mitigation, or a manually approved fix). Re-running the migration would
silently stomp that operator change and replace their approved
playwright_code.

_should_migrate_source() is a pure function extracted specifically so this
guard is unit-testable without ever touching the live county_sources row
the real migration applies against (that row is a real, hardcoded-WHERE
production key with no fake/test variant available — this repo's `.env`
DATABASE_URL is live prod, so a test that ran migrations.apply_nodriver_
scrape_modes.run() directly would be operating on real Pinellas liens
config).
"""
from migrations.apply_nodriver_scrape_modes import _should_migrate_source, _PRE_MIGRATION_MODE


def test_pre_migration_mode_is_migrated():
    assert _should_migrate_source("playwright_then_ai") is True


def test_already_migrated_mode_is_not_remigrated():
    assert _should_migrate_source("nodriver_then_ai") is False


def test_operator_chosen_ai_only_is_preserved():
    """The exact scenario from PR review: an operator switches the source
    to ai_only as a mitigation. Re-running the migration must not revert
    it."""
    assert _should_migrate_source("ai_only") is False


def test_operator_chosen_playwright_only_is_preserved():
    assert _should_migrate_source("playwright_only") is False


def test_unrecognized_mode_is_preserved_not_assumed_safe_to_overwrite():
    """Default-deny: any mode this migration doesn't explicitly recognize
    as the known pre-migration state is left alone, never overwritten."""
    assert _should_migrate_source("static_download") is False
    assert _should_migrate_source("api") is False


def test_pre_migration_mode_constant_matches_the_guard():
    # Locks the two together so a future edit to one can't silently
    # decouple the guard from the constant it's documented against.
    assert _should_migrate_source(_PRE_MIGRATION_MODE) is True
