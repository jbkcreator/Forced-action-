"""P0.2 proof only: a deliberately failing assertion. This PR is closed without merging."""


def test_ci_gate_blocks_a_failing_assertion():
    assert 1 == 2, "deliberate failure: the CI test gate must stop this from deploying"
