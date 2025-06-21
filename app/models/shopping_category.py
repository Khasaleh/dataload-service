from sqlalchemy import Column, BigInteger, String, DateTime, ForeignKey
from sqlalchemy.orm import relationship
# from sqlalchemy.sql.expression import nextval # Removed incorrect import
from app.db.base_class import Base # Assuming Base is accessible here
from app.db.models import PUBLIC_SCHEMA # Assuming PUBLIC_SCHEMA is accessible

class ShoppingCategoryOrm(Base):
    __tablename__ = "shopping_categories"
    __table_args__ = ({"schema": PUBLIC_SCHEMA})

    id = Column(BigInteger, primary_key=True, autoincrement=True) # nextval handled by DB
    name = Column(String(150), nullable=False, index=True) # For lookup

    # Optional: other fields from DDL if ever needed by the application directly
    created_at = Column(DateTime, nullable=True)
    updated_at = Column(DateTime, nullable=True)
    parent_id = Column(BigInteger, ForeignKey(f"{PUBLIC_SCHEMA}.shopping_categories.id"), nullable=True)
    business_type = Column(String(255), nullable=True)

    # Relationship to self for parent/child
    parent = relationship("ShoppingCategoryOrm", remote_side=[id], backref="children_categories") # Changed backref name to avoid potential conflict if 'children' is used elsewhere

    # Relationship to products
    # products = relationship("ProductOrm", back_populates="shopping_category")
    # This will be defined in ProductOrm using shopping_category_id.
    # The back_populates here would be "shopping_category"

    def __repr__(self):
        return f"<ShoppingCategoryOrm(id={self.id}, name='{self.name}')>"
