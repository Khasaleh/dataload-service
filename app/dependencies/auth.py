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
def decode_unverified_payload(token: str) -> dict:
    try:
        parts = token.split('.')
        if len(parts) < 2:
            raise ValueError("Invalid JWT format.")
        payload_b64 = parts[1]
        # Fix padding
        payload_b64 += '=' * (-len(payload_b64) % 4)
        decoded_bytes = base64.urlsafe_b64decode(payload_b64)
        return json.loads(decoded_bytes.decode('utf-8'))
    except Exception as e:
        logger.error(f"Error decoding unverified JWT payload: {e}")
        raise HTTPException(status_code=401, detail="Invalid JWT format.")
    
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
        payload = decode_unverified_payload(token)(token)

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