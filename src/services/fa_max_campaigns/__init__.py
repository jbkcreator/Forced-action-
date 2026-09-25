"""FA Max WP-T3-4 — Campaign Selection Agent.

Traffic-controller package: decides which of the three v1 campaigns
(capital_desk_loop, exit_desk, rescue_circuit) a person belongs to, tracks
their step and next due touch, and hands due touches to the Outreach Agent
(WP-T3-5) or Partner Nurture Agent (WP-T3-6). Contains no send code and
never writes consent — see
tasks/FA_Max_build/WP-T3-4_Campaign_Selection_Agent_Implementation_Plan.md.
"""
