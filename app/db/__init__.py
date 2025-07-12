# app/db/__init__.py

# Import Base from base_class, making it accessible via app.db.Base
from .base_class import Base

# Import all ORM models so they are registered with SQLAlchemy's metadata
# and can be easily imported from app.db (e.g., from app.db import ProductOrm)
from .models import UploadSessionOrm
from .models import BrandOrm
from .models import AttributeOrm
from .models import ReturnPolicyOrm
from .models import ProductOrm
# from .models import ProductItemOrm # Commented out as ProductItemOrm is deprecated
from .models import ProductPriceOrm
from .models import MetaTagOrm
from .models import CategoryOrm
from .models import CategoryAttributeOrm
from .models import AttributeValueOrm # Added
from .models import BusinessDetailsOrm # Added

# You can also define an __all__ variable if you want to control what `from app.db import *` imports,
# though explicit imports are generally preferred.
# __all__ = [
#     "Base",
#     "UploadSessionOrm",
#     "BrandOrm",
#     "AttributeOrm",
#     "ReturnPolicyOrm",
#     "ProductOrm",
#     # "ProductItemOrm", # Commented out
#     "ProductPriceOrm",
#     "MetaTagOrm",
#     "CategoryOrm",
#     "CategoryAttributeOrm",
#     "AttributeValueOrm", # Added
#     "BusinessDetailsOrm", # Added
# ]
