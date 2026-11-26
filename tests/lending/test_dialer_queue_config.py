"""R1 1.6: dialer queue and scorecard config."""
from config.lending_dialer_queue import (
    CALLBACKS, LF_LEADS, QUALITY_CHECKS, QUEUE_SOURCES, SOURCE_LABELS, WEB_LEADS,
)


def test_every_source_has_a_label_and_sources_are_unique():
    assert len(set(QUEUE_SOURCES)) == len(QUEUE_SOURCES)
    assert set(SOURCE_LABELS) == set(QUEUE_SOURCES)


def test_callbacks_and_warm_leads_come_before_pools():
    assert QUEUE_SOURCES[:3] == (CALLBACKS, LF_LEADS, WEB_LEADS)


def test_rubric_has_five_checks():
    assert len(QUALITY_CHECKS) == 5
