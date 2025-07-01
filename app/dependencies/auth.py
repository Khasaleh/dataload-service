from jose import JWTError, jwt
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
import logging
from typing import Optional, List, Dict, Any
from app.core.config import settings # Import centralized settings

logger = logging.getLogger(__name__)

# The tokenUrl points to the endpoint where a client can obtain a token.
# For this GraphQL-centric app, it should ideally point to the GraphQL endpoint
# if token generation happens there, or be aligned with how Postman/clients are set up.
# Using settings.API_PREFIX which defaults to /graphql.
# If a dedicated token REST endpoint existed, it would be like "/api/token".
oauth2_scheme = OAuth2PasswordBearer(tokenUrl=settings.API_PREFIX) # Assuming token generation is via GraphQL at API_PREFIX

# JWT Configuration from centralized settings
SECRET_KEY = settings.JWT_SECRET
ALGORITHM = settings.JWT_ALGORITHM

if SECRET_KEY == "your-default-secret-key-if-not-set" and settings.ENVIRONMENT != "test": # Avoid warning in tests using default
    logger.warning(
        "Using default JWT_SECRET. This should be overridden via an environment variable "
        "(JWT_SECRET or SECRET_KEY) with a strong, random key for production."
    )

def get_current_user(token: str = Depends(oauth2_scheme)) -> Dict[str, Any]:
    """
    Decodes the JWT token, extracts user claims, and returns them as a dictionary.
    Raises HTTPException if the token is invalid or essential claims are missing.
    """
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    missing_claims_exception = HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="Invalid token: Missing or invalid essential claims (e.g., sub, userId, parsable companyId).",
    )
    token_missing_business_details_exception = HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST, # Or 401/403 depending on desired strictness
        detail="Token is missing required business details.",
    )

    try:
        if settings.JWT_VERIFICATION_ENABLED:
            payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        else:
            # Decode without signature verification
            # HS256/HS512 still require a key for structural decoding even if not verifying signature.
            # If no SECRET_KEY is set in .env when verification is disabled, this might error.
            # Consider if a dummy key or specific handling is needed if SECRET_KEY can be truly absent.
            # For now, assume SECRET_KEY is always present in settings, even if it's a default/dummy one.
            if not SECRET_KEY:
                logger.error("JWT_SECRET is not configured, cannot decode token even with verification disabled.")
                raise credentials_exception # Or a more specific configuration error

            logger.warning("JWT signature verification is DISABLED. Decoding token without checking signature.")
            try:
                payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM], options={"verify_signature": False})
            except JWTError as e: # Catch errors during decoding even without signature check (e.g., malformed token)
                logger.error(f"JWTError decoding token (verification disabled): {e}")
                raise credentials_exception


        username: Optional[str] = payload.get("sub")
        user_id: Optional[Any] = payload.get("userId") # Assuming userId can be int or string from token
        company_id_str: Optional[str] = payload.get("companyId") # Original string from token
        role_array: Optional[List[Dict[str, str]]] = payload.get("role")

        business_details_id: Optional[int] = None
        if company_id_str:
            parts = company_id_str.split('-')
            # Expected format from token: FAZ-{userId}-{businessId}-{year}-{month}-{random}
            # So, the integer businessId (business_details_id) is at index 2.
            if len(parts) >= 3: # Check for at least 3 parts to safely access index 2
                try:
                    business_details_id = int(parts[2])
                except ValueError:
                    logger.warning(f"Invalid format for businessId part in companyId: '{parts[2]}' from companyIdString '{company_id_str}'. Cannot parse to int.")
                    # This is treated as a critical failure as business_details_id is essential.
                    raise missing_claims_exception
            else:
                logger.warning(f"companyIdString '{company_id_str}' does not have enough parts (expected format like FAZ-userId-businessId-year-month-random) to extract business_details_id.")
                # This is also critical if business_details_id is essential.
                raise missing_claims_exception

        # Essential claims check
        if username is None or user_id is None: # Check for username and user_id first
            logger.warning(
                f"Token missing username or userId. Username: {username}, UserID: {user_id}. Payload: {payload}"
            )
            raise missing_claims_exception

        if business_details_id is None: # Specific check for business_details_id
            logger.warning(
                f"Token failed to provide valid business_details_id. "
                f"Original CompanyIDStr: {company_id_str}, Parsed BusinessDetailsID: {business_details_id}. "
                f"Payload: {payload}"
            )
            # Raise the more specific exception if business_id is the issue
            raise token_missing_business_details_exception

        roles: List[str] = []
        if isinstance(role_array, list):
            for auth_obj in role_array:
                if isinstance(auth_obj, dict) and "authority" in auth_obj:
                    roles.append(str(auth_obj["authority"]))

        # If roles list is empty and role is strictly expected, could also raise an exception here
        # or assign a default role. For now, an empty list is permissible.

        return {
            "username": username,
            "user_id": user_id, # Consider standardizing type if necessary (e.g., always int or always str)
            "business_id": business_details_id, # This is now the parsed integer ID
            "company_id_str": company_id_str,   # Original companyId string, kept for reference if needed
            "roles": roles
        }
    except JWTError as e:
        logger.error(f"JWTError decoding token: {e}")
        raise credentials_exception
    except Exception as e: # Catch any other unexpected errors during claim processing
        logger.error(f"Unexpected error processing token claims: {e}", exc_info=True)
        raise credentials_exception