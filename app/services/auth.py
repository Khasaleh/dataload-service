import logging
from fastapi import HTTPException, status, Request
from jose import jwt, JWTError
from typing import Dict, Any, List, Optional
from app.core.config import settings

logger = logging.getLogger(__name__)

SECRET_KEY = settings.JWT_SECRET
ALGORITHM = settings.JWT_ALGORITHM

async def get_current_user(request: Request) -> Dict[str, Any]:
    """
    Dependency to extract user info from Bearer token, with optional signature validation.
    If AUTH_VALIDATION_ENABLED is False, it will skip signature verification but still
    require and parse the token.
    """

    # Always REQUIRE the Authorization header
    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.lower().startswith("bearer "):
        logger.warning("Missing or invalid Authorization header.")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing or invalid Authorization header. Bearer token required."
        )

    token = auth_header[7:].strip()

    try:
        if settings.AUTH_VALIDATION_ENABLED:
            logger.debug("JWT signature verification is ENABLED.")
            payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        else:
            logger.warning("AUTH_VALIDATION_ENABLED is False. Skipping signature verification.")
            payload = jwt.get_unverified_claims(token)

        logger.debug(f"Decoded JWT payload: {payload}")

    except JWTError as e:
        logger.error(f"JWTError decoding token: {e}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token. Unable to decode."
        )
    except Exception as e:
        logger.error(f"Unexpected error decoding token: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token. Unexpected decoding error."
        )

    # Extract required claims
    username: Optional[str] = payload.get("sub")
    user_id: Optional[Any] = payload.get("userId")
    company_id_str: Optional[str] = payload.get("companyId")
    role_array: Optional[List[Dict[str, str]]] = payload.get("role")

    # Parse business_details_id from companyId string
    business_details_id: Optional[int] = None
    if company_id_str:
        parts = company_id_str.split('-')
        if len(parts) >= 3:
            try:
                business_details_id = int(parts[2])
            except ValueError:
                logger.error(f"Invalid businessId format in companyId: {company_id_str}")
        else:
            logger.error(f"companyId string too short to parse businessId: {company_id_str}")

    if username is None or user_id is None or business_details_id is None:
        logger.error(
            f"Missing required claims. Username: {username}, userId: {user_id}, "
            f"businessId: {business_details_id}, companyId: {company_id_str}"
        )
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Token is missing required claims (sub, userId, companyId)."
        )

    # Extract roles as list of strings
    roles: List[str] = []
    if isinstance(role_array, list):
        for role_obj in role_array:
            if isinstance(role_obj, dict) and "authority" in role_obj:
                roles.append(str(role_obj["authority"]))

    user_data = {
        "username": username,
        "user_id": user_id,
        "business_id": business_details_id,
        "company_id_str": company_id_str,
        "roles": roles
    }

    logger.debug(f"Returning user_data from get_current_user: {user_data}")
    return user_data
