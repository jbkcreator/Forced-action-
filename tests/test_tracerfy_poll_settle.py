"""Regression tests for the Tracerfy queue-poll completion gate.

Bug: _poll_trace_queue accepted a transient early count-plateau as "complete",
returning 1 row while Tracerfy was still streaming the other ~66 hits — which
were billed but never ingested. Fix: require a settle window since first data,
reset on growth (Fix A).
"""
import src.services.tracerfy_fallback as tf


class _Resp:
    def __init__(self, rows):
        self._rows = rows
        self.ok = True
        self.status_code = 200

    def json(self):
        return self._rows


def _seq_get(sequence):
    it = iter(sequence)
    last = sequence[-1]

    def _get(url, headers=None, timeout=None):
        nonlocal last
        try:
            last = next(it)
        except StopIteration:
            pass
        return _Resp(last)

    return _get


def _fake_clock(monkeypatch):
    state = {"t": 0.0}
    monkeypatch.setattr(tf.time, "monotonic", lambda: state["t"])
    monkeypatch.setattr(tf.time, "sleep", lambda s: state.__setitem__("t", state["t"] + s))
    return state


def test_early_plateau_then_stream_is_not_accepted_early(monkeypatch):
    state = _fake_clock(monkeypatch)
    one = [{"address": "A", "zip": "1"}]
    full = [{"address": f"A{i}", "zip": "1"} for i in range(67)]
    # Plateau at 1 row for 4 polls, then results stream in to 67.
    seq = [one, one, one, one] + [full] * 8
    monkeypatch.setattr(tf.requests, "get", _seq_get(seq))

    out = tf._poll_trace_queue("Q", "k", estimated_wait=0,
                               stable_rounds_required=4, min_settle_seconds=30)
    assert len(out) == 67  # must not return the early 1-row plateau


def test_stable_small_queue_still_completes(monkeypatch):
    _fake_clock(monkeypatch)
    three = [{"address": f"A{i}", "zip": "1"} for i in range(3)]
    monkeypatch.setattr(tf.requests, "get", _seq_get([three] * 40))

    out = tf._poll_trace_queue("Q", "k", estimated_wait=0,
                               stable_rounds_required=4, min_settle_seconds=30)
    assert len(out) == 3


def test_settle_window_blocks_acceptance_before_elapsed(monkeypatch):
    state = _fake_clock(monkeypatch)
    one = [{"address": "A", "zip": "1"}]
    monkeypatch.setattr(tf.requests, "get", _seq_get([one] * 40))

    tf._poll_trace_queue("Q", "k", estimated_wait=0,
                         stable_rounds_required=2, min_settle_seconds=30)
    # Even a from-start-stable queue is not accepted until the settle window
    # has elapsed: clock must have advanced at least min_settle_seconds.
    assert state["t"] >= 30
