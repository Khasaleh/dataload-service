from sqlalchemy import Column, BigInteger, String, DateTime, ForeignKey, UniqueConstraint, Index
from sqlalchemy.orm import relationship
from app.db.base_class import Base
from app.db.schema_names import PUBLIC_SCHEMA
from sqlalchemy.sql import func
class ShoppingCategoryOrm(Base):
    __tablename__ = "shopping_categories"
    __table_args__ = (
        UniqueConstraint('business_details_id', 'name', name='uq_shopping_category_business_name'),
        {"schema": PUBLIC_SCHEMA},
    )

    id = Column(BigInteger, primary_key=True, index=True, autoincrement=True)
    name = Column(String(150), nullable=False, index=True)
    parent_id = Column(BigInteger, ForeignKey(f"{PUBLIC_SCHEMA}.shopping_categories.id"), nullable=True)
    business_details_id = Column(BigInteger, nullable=False, index=True)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, onupdate=func.now(), nullable=True)

    # Relationship with BusinessDetailsOrm
    business_detail = relationship("BusinessDetailsOrm", back_populates="shopping_categories")

    # Self-referential parent-child hierarchy
    parent = relationship(
        "ShoppingCategoryOrm",
        remote_side=[id],
        back_populates="children",
    )

    children = relationship(
        "ShoppingCategoryOrm",
        back_populates="parent",
        cascade="all, delete-orphan",
        single_parent=True,
    )

    def __repr__(self):
        return f"<ShoppingCategoryOrm(id={self.id}, name='{self.name}', parent_id={self.parent_id})>"
