import strawberry
import datetime
import uuid # For strawberry.ID or if we use uuid.UUID directly
from typing import Optional, List # List might be used for future types

# Note: Strawberry automatically handles conversion for many Python built-in types
# (str, int, float, bool, datetime.datetime, uuid.UUID) to corresponding GraphQL scalars.
# For UUIDs, strawberry.ID is often used for GraphQL schema IDs.

@strawberry.type
class UploadSessionType:
    """
    Represents an upload session, providing status and details of a file upload.
    Corresponds to the Pydantic UploadSessionModel.
    """
    session_id: strawberry.ID  # GraphQL ID type, typically a string. Strawberry handles UUID to ID conversion.
    business_id: str
    load_type: str
    original_filename: str
    wasabi_path: str
    status: str
    details: Optional[str] = None
    record_count: Optional[int] = None
    error_count: Optional[int] = None
    created_at: datetime.datetime
    updated_at: datetime.datetime

@strawberry.type
class UserType:
    """
    Represents an authenticated user's information, typically derived from JWT claims
    processed by the get_current_user dependency.
    """
    user_id: strawberry.ID      # Corresponds to 'userId' claim from JWT. Strawberry handles various underlying types for ID.
    username: str               # Corresponds to 'sub' (subject) claim from JWT.
    business_id: str            # Corresponds to 'companyId' claim from JWT (mapped to 'business_id' in auth context).
    roles: List[str]            # List of role strings (e.g., ["ROLE_ADMIN", "ROLE_USER"]).

@strawberry.type
class TokenResponseType:
    """
    Response type for authentication token generation.
    Includes the access token and a refresh token.
    """
    token: str          # The JWT access token (often referred to as auth-token).
    token_type: str     # Typically "bearer".
    refreshToken: str   # The refresh token (often referred to as auth-refreshtoken).
    # Optional fields if the token generation response should directly include some user info:
    # user_id: Optional[strawberry.ID] = None
    # username: Optional[str] = None
    # roles: Optional[List[str]] = None

@strawberry.type
class StandardMessageType:
    """
    A standard message type for simple mutation responses,
    indicating success or failure with a message.
    """
    message: str
    success: bool = True # Defaults to True, can be set to False for errors.

# Regarding other Pydantic Models (BrandModel, ProductModel, etc.):
# These are primarily used for data validation within CSV processing (Celery tasks)
# and are not directly exposed via the initial set of GraphQL queries/mutations.
# If future GraphQL queries need to return these entities, corresponding Strawberry types
# (e.g., BrandType, ProductType) would be defined here, potentially using Strawberry's
# Pydantic integration features if desired (e.g., strawberry.experimental.pydantic.type).

# Regarding File Upload Scalar:
# Strawberry handles file uploads using `strawberry.file_uploads.Upload`.
# This type is used directly in mutation arguments like: `file: strawberry.file_uploads.Upload`
# No custom definition for the scalar itself is needed here.

# Regarding Input Types for Mutations (`@strawberry.input`):
# These will be defined closer to their respective mutations in the GraphQL schema/resolvers files.
# This co-location helps in understanding the expected input for each mutation directly.
# For example, an input type for token generation might be:
# @strawberry.input
# class GenerateTokenInput:
#     username: str
#     password: str # Or other credentials like client_id/client_secret for machine auth


@strawberry.enum
class PriceTypeGQL(str): # Using str as the base for Enum values
    PRODUCT = "PRODUCT"
    SKU = "SKU"

@strawberry.type
class PriceType:
    id: strawberry.ID
    business_id: str # Assuming prices are scoped by business
    product_id: Optional[strawberry.ID] = None
    sku_id: Optional[strawberry.ID] = None
    price: float
    discount_price: Optional[float] = None
    cost_price: Optional[float] = None
    currency: Optional[str] = "USD" # Default currency
    created_at: datetime.datetime
    updated_at: datetime.datetime

    # Resolver for product (if product_id is set)
    # @strawberry.field
    # async def product(self, info: strawberry.types.Info) -> Optional['ProductTypeGQL']: # Forward reference if ProductTypeGQL is defined later
    #     if self.product_id:
    #         # Logic to fetch product by self.product_id
    #         # return await get_product_by_id_loader(info).load(self.product_id)
    #         pass
    #     return None

    # Resolver for sku (if sku_id is set)
    # @strawberry.field
    # async def sku(self, info: strawberry.types.Info) -> Optional['SKUTypeGQL']: # Forward reference
    #     if self.sku_id:
    #         # Logic to fetch SKU by self.sku_id
    #         # return await get_sku_by_id_loader(info).load(self.sku_id)
    #         pass
    #     return None

@strawberry.input
class PriceInput:
    price_type: PriceTypeGQL # PRODUCT or SKU
    target_id: strawberry.ID # product_id if PRODUCT, sku_id if SKU
    price: float
    discount_price: Optional[float] = None
    cost_price: Optional[float] = None
    currency: Optional[str] = "USD"
```

