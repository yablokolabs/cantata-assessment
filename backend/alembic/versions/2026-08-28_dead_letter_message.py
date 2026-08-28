"""dead_letter_message

Revision ID: 0002_dead_letter_message
Revises: 0001_initial
Create Date: 2026-08-28

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = '0002_dead_letter_message'
down_revision: Union[str, None] = '0001_initial'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'dead_letter_message',
        sa.Column('id', postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column('pipeline_id', postgresql.UUID(as_uuid=True), sa.ForeignKey('pipeline.id'), nullable=False),
        sa.Column('step_tag', sa.String(), nullable=False),
        sa.Column('failure_class', sa.String(), nullable=False),
        sa.Column('exception_type', sa.String(), nullable=False),
        sa.Column('traceback', sa.Text(), nullable=True),
        sa.Column('payload', postgresql.JSONB, nullable=False, server_default='{}'),
        sa.Column('attempts', sa.Integer(), nullable=False, server_default='1'),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column('replayed_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('replay_of_id', postgresql.UUID(as_uuid=True), sa.ForeignKey('dead_letter_message.id'), nullable=True),
        sa.Column('discarded_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('discard_reason', sa.Text(), nullable=True),
        sa.CheckConstraint(
            "failure_class IN ('TRANSIENT','POISON','NEEDS_HUMAN','UNKNOWN')",
            name='ck_dlq_failure_class',
        ),
    )
    op.create_index('ix_dlq_pipeline', 'dead_letter_message', ['pipeline_id'])
    op.create_index('ix_dlq_created', 'dead_letter_message', [sa.text('created_at DESC')])
    # Partial index over unresolved rows only — this is the operator's hot query,
    # and it stays fast regardless of how large the table grows.
    op.create_index(
        'ix_dlq_open_class',
        'dead_letter_message',
        ['failure_class'],
        postgresql_where=sa.text('replayed_at IS NULL AND discarded_at IS NULL'),
    )


def downgrade() -> None:
    op.drop_index('ix_dlq_open_class', table_name='dead_letter_message')
    op.drop_index('ix_dlq_created', table_name='dead_letter_message')
    op.drop_index('ix_dlq_pipeline', table_name='dead_letter_message')
    op.drop_table('dead_letter_message')
