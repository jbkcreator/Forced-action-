"""R1 0.9 Shadow mode harness.

Sub-modules:
  registry   — maps class names to their decision-maker callables + comparators
  judge      — auto-send-judge-baseline and auto-send-judge-v1 (pure rules, no paid API)
  store      — the ONLY module that writes shadow tables
  shadow_runner — ShadowRunnerStep sweep step + CLI (--start / --stop)
"""
