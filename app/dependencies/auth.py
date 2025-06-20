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
        status_code=status.HTTP_403_FORBIDDEN, # Using 403 as claims are there, but content is bad for our app
        detail="Invalid token: Missing essential claims (sub, userId, companyId).",
    )

    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])

        username: Optional[str] = payload.get("sub")
        user_id: Optional[Any] = payload.get("userId") # Keep Any for now, can be int or str depending on source
        company_id: Optional[str] = payload.get("companyId")
        role_array: Optional[List[Dict[str, str]]] = payload.get("role")

        if username is None or user_id is None or company_id is None:
            logger.warning(f"Token missing one or more core claims: sub, userId, companyId. Payload: {payload}")
            raise missing_claims_exception # Use more specific exception for bad claims

        roles: List[str] = []
        if isinstance(role_array, list):
            for auth_obj in role_array:
                if isinstance(auth_obj, dict) and "authority" in auth_obj:
                    roles.append(str(auth_obj["authority"]))

        # If roles list is empty and role is strictly expected, could also raise an exception here
        # or assign a default role. For now, an empty list is permissible.

        return {
            "username": username,
            "user_id": user_id, # Consider converting to int(user_id) if it's always an integer string
            "business_id": company_id, # Mapping companyId to business_id for internal consistency
            "roles": roles
        }
    except JWTError as e:
        logger.error(f"JWTError decoding token: {e}")
        raise credentials_exception
    except Exception as e: # Catch any other unexpected errors during claim processing
        logger.error(f"Unexpected error processing token claims: {e}", exc_info=True)
        raise credentials_exception