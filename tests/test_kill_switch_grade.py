"""Phase 1 tests — grade() helper and engine re-export."""
from src.services.kill_switch_grade import grade


class TestGrade:
    # higher_is_better metrics
    def test_green_at_threshold(self):
        # first_payment_rate green=30
        assert grade("first_payment_rate", 30.0) == "green"

    def test_green_above_threshold(self):
        assert grade("first_payment_rate", 50.0) == "green"

    def test_red_below_threshold(self):
        # red=20 → <20 is red
        assert grade("first_payment_rate", 19.9) == "red"

    def test_yellow_between(self):
        # 20 <= x < 30 → yellow
        assert grade("first_payment_rate", 25.0) == "yellow"

    def test_exactly_red_boundary_is_yellow(self):
        # observed == red threshold (20) → NOT red (red means < 20)
        assert grade("first_payment_rate", 20.0) == "yellow"

    # lower_is_better metrics
    def test_lower_is_better_green(self):
        # cac_paid_channels: green=25 (<=25 is green)
        assert grade("cac_paid_channels", 20.0) == "green"

    def test_lower_is_better_red(self):
        # red=40 → >40 is red
        assert grade("cac_paid_channels", 45.0) == "red"

    def test_lower_is_better_yellow(self):
        assert grade("cac_paid_channels", 30.0) == "yellow"

    # Edge cases
    def test_none_observed_returns_unknown(self):
        assert grade("first_payment_rate", None) == "unknown"

    def test_unknown_metric_returns_unknown(self):
        assert grade("nonexistent_metric", 50.0) == "unknown"


class TestEngineReexport:
    def test_engine_grade_is_reexport(self):
        """cora_self_healing._grade is the same function as kill_switch_grade.grade."""
        from src.tasks.cora_self_healing import _grade
        assert _grade is grade
