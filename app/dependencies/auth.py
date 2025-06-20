from jose import JWTError, jwt
from fastapi import Depends, HTTPException, status # Added status
from fastapi.security import OAuth2PasswordBearer
import os
import logging # Added logging
from typing import Optional, List, Dict, Any # For type hints

logger = logging.getLogger(__name__) # Initialize logger

# The tokenUrl points to the endpoint where a client can obtain a token.
# If using GraphQL exclusively for token generation, this might point to /graphql.
# However, its primary use here is for OpenAPI documentation if you use FastAPI's built-in docs
# to authorize API calls. For programmatic calls or GraphiQL, the client just needs to send the token.
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/token") # Or potentially "/graphql" if token mutation is there

# Standardized JWT Secret environment variable
SECRET_KEY = os.getenv("JWT_SECRET", "your-default-secret-key-if-not-set")
# JWT Algorithm to be used for encoding/decoding tokens. HS512 is a stronger default.
ALGORITHM = os.getenv("JWT_ALGORITHM", "HS512")

if SECRET_KEY == "your-default-secret-key-if-not-set":
    logger.warning("Using default JWT_SECRET. This should be set via an environment variable with a strong, random key for production.")

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

    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])

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

        # Essential claims check now includes the parsed integer business_details_id
        if username is None or user_id is None or business_details_id is None:
            logger.warning(
                f"Token missing one or more core claims or failed to parse/validate business_details_id. "
                f"Username: {username}, UserID: {user_id}, Parsed BusinessDetailsID: {business_details_id}, "
                f"Original CompanyIDStr: {company_id_str}. Payload: {payload}"
            )
            raise missing_claims_exception

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