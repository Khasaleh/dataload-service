import base64
import json
import logging
from typing import Any, Dict, List, Optional

from jose import jwt, JWTError
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from app.core.config import settings

logger = logging.getLogger(__name__)

oauth2_scheme = OAuth2PasswordBearer(tokenUrl=settings.API_PREFIX)
SECRET_KEY = settings.JWT_SECRET
ALGORITHM = settings.JWT_ALGORITHM


def get_current_user(token: str = Depends(oauth2_scheme)) -> Dict[str, Any]:
    """
    Decodes the JWT token, extracts user claims, and returns them as a dictionary.
    Supports two modes:
    - AUTH_VALIDATION_ENABLED = True: Fully verifies JWT signature.
    - AUTH_VALIDATION_ENABLED = False: Decodes payload without verifying signature.
    """
    if not settings.AUTH_VALIDATION_ENABLED:
        logger.warning("AUTH_VALIDATION_ENABLED is False. Attempting to parse payload without signature verification.")

        try:
            # Split token
            parts = token.split('.')
            if len(parts) != 3:
                raise HTTPException(status_code=400, detail="Malformed JWT token. Expect 3 parts.")

            payload_b64 = parts[1]
            # Add padding if needed
            padding_needed = 4 - (len(payload_b64) % 4)
            if padding_needed and padding_needed < 4:
                payload_b64 += "=" * padding_needed

            # Decode
            decoded_bytes = base64.urlsafe_b64decode(payload_b64.encode())
            decoded_str = decoded_bytes.decode('utf-8')
            payload = json.loads(decoded_str)

            logger.debug(f"Decoded JWT payload without signature verification: {payload}")

            # Extract
            username = payload.get("sub")
            user_id = payload.get("userId")
            company_id_str = payload.get("companyId")
            role_array = payload.get("role")

            if not company_id_str:
                raise HTTPException(status_code=403, detail="Missing companyId in token payload.")

            # Parse businessId from companyId
            parts = company_id_str.split('-')
            if len(parts) < 3:
                raise HTTPException(
                    status_code=403,
                    detail=f"Invalid companyId format: {company_id_str}"
                )

            parsed_user_id = int(parts[1])
            parsed_business_id = int(parts[2])

            # Extract roles
            roles: List[str] = []
            if isinstance(role_array, list):
                for auth_obj in role_array:
                    if isinstance(auth_obj, dict) and "authority" in auth_obj:
                        roles.append(str(auth_obj["authority"]))

            user_data = {
                "username": username,
                "user_id": parsed_user_id,
                "business_id": parsed_business_id,
                "company_id_str": company_id_str,
                "roles": roles
            }

            logger.debug(f"Parsed user data without verification: {user_data}")
            return user_data

        except (ValueError, json.JSONDecodeError) as e:
            logger.error(f"Error decoding JWT payload: {e}", exc_info=True)
            raise HTTPException(status_code=400, detail="Invalid JWT payload format.")
        except Exception as e:
            logger.error(f"Unexpected error in JWT parsing: {e}", exc_info=True)
            raise HTTPException(status_code=400, detail="Invalid JWT token.")

    # If AUTH_VALIDATION_ENABLED is True → fully validate signature
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])

        username = payload.get("sub")
        user_id = payload.get("userId")
        company_id_str = payload.get("companyId")
        role_array = payload.get("role")

        if not company_id_str:
            raise HTTPException(status_code=403, detail="Missing companyId in token.")

        parts = company_id_str.split('-')
        if len(parts) < 3:
            raise HTTPException(status_code=403, detail=f"Invalid companyId format: {company_id_str}")

        parsed_user_id = int(parts[1])
        parsed_business_id = int(parts[2])

        roles: List[str] = []
        if isinstance(role_array, list):
            for auth_obj in role_array:
                if isinstance(auth_obj, dict) and "authority" in auth_obj:
                    roles.append(str(auth_obj["authority"]))

        return {
            "username": username,
            "user_id": parsed_user_id,
            "business_id": parsed_business_id,
            "company_id_str": company_id_str,
            "roles": roles
        }

    except JWTError as e:
        logger.error(f"JWTError decoding token: {e}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )
