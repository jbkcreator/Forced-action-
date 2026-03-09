"""m1_county_id_hub_tables_and_new_m1_tables

Revision ID: c1d2e3f4a5b6
Revises: a1b2c3d4e5f6
Create Date: 2026-03-10

Two changes in one migration:

1. Add county_id to the 4 hub/core tables that were missed in the previous migration:
   - properties
   - owners
   - financials
   - distress_scores

2. Create 4 new M1 revenue/subscriber tables:
   - founding_subscriber_counts
   - subscribers
   - zip_territories
   - enriched_contacts
"""
from typing import Sequence, Union

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from alembic import op

revision: str = 'c1d2e3f4a5b6'
down_revision: Union[str, Sequence[str], None] = 'b1c2d3e4f5a6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

# Hub tables missing county_id
HUB_TABLES = ['properties', 'owners', 'financials', 'distress_scores']


def upgrade() -> None:
    # ── 1. Add county_id to hub tables ────────────────────────────────────────
    for table in HUB_TABLES:
        op.add_column(table, sa.Column('county_id', sa.String(50), nullable=True))
        op.create_index(f'idx_{table}_county_id', table, ['county_id'])
        op.execute(f"UPDATE {table} SET county_id = 'hillsborough' WHERE county_id IS NULL")

    # ── 2. founding_subscriber_counts ─────────────────────────────────────────
    op.create_table(
        'founding_subscriber_counts',
        sa.Column('id', sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column('tier', sa.String(20), nullable=False),
        sa.Column('vertical', sa.String(50), nullable=False),
        sa.Column('county_id', sa.String(50), nullable=False),
        sa.Column('count', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint('tier', 'vertical', 'county_id', name='uq_founding_tier_vertical_county'),
        sa.CheckConstraint("tier IN ('starter', 'pro', 'dominator')", name='check_founding_tier'),
    )
    op.create_index('idx_founding_county_id', 'founding_subscriber_counts', ['county_id'])

    # ── 3. subscribers ────────────────────────────────────────────────────────
    op.create_table(
        'subscribers',
        sa.Column('id', sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column('stripe_customer_id', sa.String(100), nullable=False, unique=True),
        sa.Column('stripe_subscription_id', sa.String(100), nullable=True, unique=True),
        sa.Column('tier', sa.String(20), nullable=False),
        sa.Column('vertical', sa.String(50), nullable=False),
        sa.Column('county_id', sa.String(50), nullable=False),
        sa.Column('founding_member', sa.Boolean(), nullable=False, server_default='false'),
        sa.Column('founding_price_id', sa.String(100), nullable=True),
        sa.Column('rate_locked_at', sa.DateTime(), nullable=True),
        sa.Column('status', sa.String(20), nullable=False, server_default='active'),
        sa.Column('billing_date', sa.DateTime(), nullable=True),
        sa.Column('grace_expires_at', sa.DateTime(), nullable=True),
        sa.Column('ghl_contact_id', sa.String(100), nullable=True),
        sa.Column('ghl_stage', sa.Integer(), nullable=True),
        sa.Column('event_feed_uuid', sa.String(36), nullable=True, unique=True),
        sa.Column('email', sa.String(255), nullable=True),
        sa.Column('name', sa.String(255), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.CheckConstraint("tier IN ('starter', 'pro', 'dominator')", name='check_subscriber_tier'),
        sa.CheckConstraint("status IN ('active', 'grace', 'churned', 'cancelled')", name='check_subscriber_status'),
    )
    op.create_index('idx_subscriber_stripe_customer', 'subscribers', ['stripe_customer_id'])
    op.create_index('idx_subscriber_stripe_sub', 'subscribers', ['stripe_subscription_id'])
    op.create_index('idx_subscriber_county_id', 'subscribers', ['county_id'])
    op.create_index('idx_subscriber_status', 'subscribers', ['status'])
    op.create_index('idx_subscriber_vertical', 'subscribers', ['vertical'])
    op.create_index('idx_subscriber_ghl_contact', 'subscribers', ['ghl_contact_id'])
    op.create_index('idx_subscriber_event_feed_uuid', 'subscribers', ['event_feed_uuid'])
    op.create_index('idx_subscriber_email', 'subscribers', ['email'])

    # ── 4. zip_territories ────────────────────────────────────────────────────
    op.create_table(
        'zip_territories',
        sa.Column('id', sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column('zip_code', sa.String(10), nullable=False),
        sa.Column('vertical', sa.String(50), nullable=False),
        sa.Column('county_id', sa.String(50), nullable=False),
        sa.Column('subscriber_id', sa.Integer(), sa.ForeignKey('subscribers.id'), nullable=True),
        sa.Column('status', sa.String(20), nullable=False, server_default='available'),
        sa.Column('locked_at', sa.DateTime(), nullable=True),
        sa.Column('grace_expires_at', sa.DateTime(), nullable=True),
        sa.Column('waitlist_emails', ARRAY(sa.String(255)), nullable=True, server_default='{}'),
        sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint('zip_code', 'vertical', 'county_id', name='uq_zip_vertical_county'),
        sa.CheckConstraint("status IN ('available', 'locked', 'grace')", name='check_zip_status'),
    )
    op.create_index('idx_zip_territory_status', 'zip_territories', ['status'])
    op.create_index('idx_zip_territory_county_id', 'zip_territories', ['county_id'])
    op.create_index('idx_zip_territory_subscriber_id', 'zip_territories', ['subscriber_id'])

    # ── 5. enriched_contacts ──────────────────────────────────────────────────
    op.create_table(
        'enriched_contacts',
        sa.Column('id', sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column('property_id', sa.Integer(), sa.ForeignKey('properties.id'), nullable=False),
        sa.Column('county_id', sa.String(50), nullable=False),
        sa.Column('mobile_phone', sa.String(20), nullable=True),
        sa.Column('landline', sa.String(20), nullable=True),
        sa.Column('email', sa.String(255), nullable=True),
        sa.Column('mailing_address', sa.String(255), nullable=True),
        sa.Column('llc_owner_name', sa.String(255), nullable=True),
        sa.Column('relative_contacts', JSONB, nullable=True),
        sa.Column('source', sa.String(50), nullable=False),
        sa.Column('match_success', sa.Boolean(), nullable=False, server_default='false'),
        sa.Column('ghl_contact_id', sa.String(100), nullable=True),
        sa.Column('ghl_synced_at', sa.DateTime(), nullable=True),
        sa.Column('enriched_at', sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.CheckConstraint("source IN ('batch_skip_tracing', 'idi')", name='check_enriched_source'),
    )
    op.create_index('idx_enriched_property_id', 'enriched_contacts', ['property_id'])
    op.create_index('idx_enriched_county_id', 'enriched_contacts', ['county_id'])
    op.create_index('idx_enriched_match_success', 'enriched_contacts', ['match_success'])
    op.create_index('idx_enriched_source', 'enriched_contacts', ['source'])
    op.create_index('idx_enriched_ghl_contact', 'enriched_contacts', ['ghl_contact_id'])


def downgrade() -> None:
    # Drop new M1 tables in reverse FK order
    op.drop_table('enriched_contacts')
    op.drop_table('zip_territories')
    op.drop_table('subscribers')
    op.drop_table('founding_subscriber_counts')

    # Remove county_id from hub tables
    for table in reversed(HUB_TABLES):
        op.drop_index(f'idx_{table}_county_id', table_name=table)
        op.drop_column(table, 'county_id')
