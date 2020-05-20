"""empty message

Revision ID: 772c1e4681b3
Revises: 483de9e1a37c
Create Date: 2020-05-20 08:43:40.043241

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.

revision = '772c1e4681b3'
down_revision = '483de9e1a37c'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    summaryresolution = postgresql.ENUM('all', 'year', 'month', 'day', 'hour', name='summaryresolution')
    summaryresolution.create(op.get_bind())

    op.add_column('disclosure', sa.Column('max_resolution', sa.Enum('all', 'year', 'month', 'day', 'hour', name='summaryresolution'), nullable=True))

    op.execute("UPDATE disclosure SET max_resolution = 'hour'")

    op.alter_column('disclosure', 'max_resolution',
               existing_type=postgresql.ENUM('all', 'year', 'month', 'day', 'hour', name='summaryresolution'),
               nullable=False)
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('disclosure', 'max_resolution')

    summaryresolution = postgresql.ENUM('all', 'year', 'month', 'day', 'hour', name='summaryresolution')
    summaryresolution.drop(op.get_bind())
    # ### end Alembic commands ###