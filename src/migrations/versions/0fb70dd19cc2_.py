"""empty message

Revision ID: 0fb70dd19cc2
Revises: 229255540a0d
Create Date: 2020-09-23 09:42:34.680829

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '0fb70dd19cc2'
down_revision = '229255540a0d'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('meteringpoint', sa.Column('disabled', sa.Boolean(), nullable=True))
    op.execute("UPDATE meteringpoint SET disabled='f';")
    op.alter_column('meteringpoint', 'disabled',
               existing_type=sa.BOOLEAN(),
               nullable=False)
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('meteringpoint', 'disabled')
    # ### end Alembic commands ###
