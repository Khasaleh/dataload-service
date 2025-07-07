from sqlalchemy import Column, BigInteger, String, DateTime, ForeignKey, UniqueConstraint, Index
from sqlalchemy.orm import relationship
from app.db.base_class import Base
from app.db.models import PUBLIC_SCHEMA

class ShoppingCategoryOrm(Base):
    __tablename__ = "shopping_categories"
    __table_args__ = (
        UniqueConstraint('name', 'parent_id', name='uq_shopping_categories_parent_name'),
        Index('idx_shopping_categories_business', 'parent_id'),
        {'extend_existing': True, 'schema': PUBLIC_SCHEMA},
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True, index=True)
    name = Column(String(150), nullable=False, index=True)

    created_at = Column(DateTime, nullable=True)
    updated_at = Column(DateTime, nullable=True)
    parent_id = Column(
        BigInteger,
        ForeignKey(f"{PUBLIC_SCHEMA}.shopping_categories.id"),
        nullable=True,
        index=True
    )
    business_type = Column(String(255), nullable=True)

    # Self‐referential relationship for hierarchical categories
    parent = relationship(
        "ShoppingCategoryOrm",
        remote_side=[id],
        backref="children_categories",
        cascade="all, delete-orphan",
    )

    def __repr__(self):
        return f"<ShoppingCategoryOrm(id={self.id}, name='{self.name}', parent_id={self.parent_id})>"
