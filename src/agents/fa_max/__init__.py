"""FA Max agent runtime (WP-T2-2) — bounded LangGraph tool-call loop consuming
``fa_max_work_queue`` (see src.services.state_engine's claim_next_work_item /
complete_work_item / reclaim_expired_work_items).

This is a separate consumer/process from the Lifecycle Redis-stream runtime
under src.agents (supervisor/router/graphs) and from Cora (src.agents.cora,
its own Redis-stream consumer). Entry point: ``python -m src.agents.fa_max.worker``.
"""
