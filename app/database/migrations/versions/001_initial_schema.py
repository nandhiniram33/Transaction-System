"""Initial schema for transactions and outbox events

Revision ID: 001_initial_schema
Revises: 
Create Date: 2026-10-12 12:00:00.000000

"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa

revision: str = '001_initial_schema'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Create transactions table
    op.create_table(
        'transactions',
        sa.Column('id', sa.String(length=36), primary_key=True),
        sa.Column('amount', sa.Numeric(precision=18, scale=4), nullable=False),
        sa.Column('currency', sa.String(length=3), nullable=False),
        sa.Column('description', sa.String(length=500), nullable=False),
        sa.Column('timestamp', sa.DateTime(timezone=True), nullable=False),
        sa.Column('metadata_json', sa.Text(), nullable=True),
        sa.Column('payload_fingerprint', sa.String(length=64), nullable=False),
        sa.Column('status', sa.String(length=20), nullable=False, server_default='pending'),
        sa.Column('submitted_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('completed_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('error', sa.Text(), nullable=True),
        sa.Column('worker_id', sa.String(length=100), nullable=True),
        sa.Column('lease_expires_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('retry_count', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('next_retry_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('last_attempt_at', sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index('ix_transactions_id', 'transactions', ['id'])
    op.create_index('ix_transactions_status', 'transactions', ['status'])
    op.create_index('ix_transactions_payload_fingerprint', 'transactions', ['payload_fingerprint'])
    op.create_index('ix_transactions_lease_expires_at', 'transactions', ['lease_expires_at'])
    op.create_index('ix_transactions_next_retry_at', 'transactions', ['next_retry_at'])
    op.create_index('ix_transactions_status_lease', 'transactions', ['status', 'lease_expires_at'])
    op.create_index('ix_transactions_status_retry', 'transactions', ['status', 'next_retry_at'])

    # Create outbox_events table
    op.create_table(
        'outbox_events',
        sa.Column('id', sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column('transaction_id', sa.String(length=36), nullable=False),
        sa.Column('event_type', sa.String(length=50), nullable=False, server_default='TRANSACTION_CREATED'),
        sa.Column('payload', sa.Text(), nullable=False),
        sa.Column('status', sa.String(length=20), nullable=False, server_default='pending'),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('dispatched_at', sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index('ix_outbox_transaction_id', 'outbox_events', ['transaction_id'])
    op.create_index('ix_outbox_status', 'outbox_events', ['status'])
    op.create_index('ix_outbox_status_created', 'outbox_events', ['status', 'created_at'])


def downgrade() -> None:
    op.drop_table('outbox_events')
    op.drop_table('transactions')
