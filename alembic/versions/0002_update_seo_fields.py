"""update_seo_fields_in_product_and_category

Revision ID: 0002
Revises: 0001
Create Date: 2024-08-01 12:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# Schema constants
PUBLIC_SCHEMA = "public" # Assuming this is the correct schema for products and categories as per current models

# revision identifiers, used by Alembic.
revision: str = '0002'
down_revision: Union[str, None] = '0001'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands for Product table ###
    # These columns are new to the products table based on the requirements.
    # Assuming products table is in PUBLIC_SCHEMA as per current ProductOrm model.
    op.add_column('products', sa.Column('keywords', sa.String(length=512), nullable=True), schema=PUBLIC_SCHEMA)
    op.add_column('products', sa.Column('seo_description', sa.String(length=512), nullable=True), schema=PUBLIC_SCHEMA)
    op.add_column('products', sa.Column('seo_title', sa.String(length=256), nullable=True), schema=PUBLIC_SCHEMA)

    # ### commands for Category table ###
    # According to app/db/models.py and alembic/versions/0001_create_initial_tables.py,
    # the columns seo_title, seo_description, and seo_keywords on the categories table
    # should already exist with the specified lengths (String(255)) in the PUBLIC_SCHEMA.
    # Therefore, no direct schema changes (add_column or alter_column for length) are made here for categories.
    # If these fields were missing or had incorrect types/lengths despite model/0001,
    # then appropriate op.add_column or op.alter_column calls would be needed.
    # For this migration, we assume 0001 correctly established these category SEO fields.
    pass


def downgrade() -> None:
    # ### commands for Product table ###
    op.drop_column('products', 'seo_title', schema=PUBLIC_SCHEMA)
    op.drop_column('products', 'seo_description', schema=PUBLIC_SCHEMA)
    op.drop_column('products', 'keywords', schema=PUBLIC_SCHEMA)

    # ### commands for Category table ###
    # No schema changes were made to the categories table in the upgrade() function of this migration.
    pass
