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

# Custom Exceptions for token processing
class TokenError(Exception):
    """Base class for token-related errors."""
    pass

class ExpiredTokenError(TokenError):
    """Raised when a token has expired."""
    pass

class InvalidTokenError(TokenError):
    """Raised when a token is invalid (e.g., bad signature, malformed)."""
    pass

class MissingClaimsError(InvalidTokenError):
    """Raised when essential claims are missing or invalid in the token."""
    pass


if SECRET_KEY == "your-default-secret-key-if-not-set" and settings.ENVIRONMENT != "test": # Avoid warning in tests using default
    logger.warning(
        "Using default JWT_SECRET. This should be overridden via an environment variable "
        "(JWT_SECRET or SECRET_KEY) with a strong, random key for production."
    )

def _parse_jwt_token_data(token: str) -> Dict[str, Any]:
    """
    Core logic to decode JWT, parse claims, and validate.
    Raises custom token exceptions on failure.
    """
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])

        username: Optional[str] = payload.get("sub")
        user_id: Optional[Any] = payload.get("userId")
        company_id_str: Optional[str] = payload.get("companyId")
        role_array: Optional[List[Dict[str, str]]] = payload.get("role")

        if not company_id_str:
            logger.warning("companyId missing from token payload")
            raise MissingClaimsError("Invalid token: companyId missing")

        business_details_id: Optional[int] = None
        try:
            parts = company_id_str.split('-')
            if len(parts) >= 3:
                business_details_id = int(parts[2])
            else:
                logger.warning(f"companyIdString '{company_id_str}' does not have enough parts to extract business_id.")
                raise MissingClaimsError("Invalid token: companyId format error (not enough parts)")
        except ValueError:
            logger.warning(f"Invalid format for businessId part in companyId: '{parts[2]}' from companyIdString '{company_id_str}'.")
            raise MissingClaimsError("Invalid token: companyId format error (parsing business_id part)")
        except IndexError: # Should be caught by len(parts) check, but as safeguard
            logger.warning(f"Index error while parsing companyIdString '{company_id_str}'.")
            raise MissingClaimsError("Invalid token: companyId format error (index error)")


        if username is None or user_id is None or business_details_id is None: # business_details_id must be parsed successfully
            logger.warning(
                f"Token missing one or more core claims or failed to parse/validate business_details_id. "
                f"Username: {username}, UserID: {user_id}, Parsed BusinessDetailsID: {business_details_id}, "
                f"Original CompanyIDStr: {company_id_str}."
            )
            raise MissingClaimsError("Invalid token: Essential claims missing or invalid (username, userId, or parsable companyId)")

        roles: List[str] = []
        if isinstance(role_array, list):
            for auth_obj in role_array:
                if isinstance(auth_obj, dict) and "authority" in auth_obj:
                    roles.append(str(auth_obj["authority"]))

        return {
            "username": username,
            "user_id": user_id,
            "business_id": business_details_id,
            "company_id_str": company_id_str,
            "roles": roles
        }
    except jwt.ExpiredSignatureError: # More specific exception from python-jose
        logger.error("Token has expired")
        raise ExpiredTokenError("Token has expired")
    except jwt.JWTClaimsError as e: # For issues with claims like nbf, iat, exp if they are malformed by jose itself
        logger.error(f"Token claims validation error by jose: {e}")
        raise InvalidTokenError(f"Invalid token: Claims error ({str(e)})")
    except JWTError as e: # Catch-all for other jose JWT errors (e.g. bad signature, malformed token)
        logger.error(f"JWTError decoding token: {e}")
        raise InvalidTokenError(f"Invalid token ({str(e)})")
    except MissingClaimsError: # Re-raise specific custom exceptions
        raise
    except Exception as e: # Catch any other unexpected errors during claim processing
        logger.error(f"Unexpected error processing token claims: {e}", exc_info=True)
        raise TokenError(f"Error processing token: {str(e)}")


def get_current_user(token: str = Depends(oauth2_scheme)) -> Dict[str, Any]:
    """
    FastAPI Dependency: Decodes the JWT token using the shared utility,
    extracts user claims, and returns them as a dictionary.
    Translates custom token exceptions to HTTPException.
    """
    try:
        user_data = _parse_jwt_token_data(token)
        return user_data
    except ExpiredTokenError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token has expired",
            headers={"WWW-Authenticate": "Bearer"},
        )
    except MissingClaimsError as e: # Handles specific issues with claims content or parsing
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, # Or 401, depending on policy for bad claims vs bad signature
            detail=str(e), # Use the message from MissingClaimsError
            headers={"WWW-Authenticate": "Bearer"},
        )
    except InvalidTokenError as e: # Handles general invalid token issues like signature
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=str(e), # Use the message from InvalidTokenError
            headers={"WWW-Authenticate": "Bearer"},
        )
    except TokenError as e: # Catch-all for other TokenErrors from _parse_jwt_token_data
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Could not validate credentials: {str(e)}",
            headers={"WWW-Authenticate": "Bearer"},
        )