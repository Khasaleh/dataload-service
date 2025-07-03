from jose import jwt, JWTError
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from typing import List, Dict, Any, Optional
import logging
from app.core.config import settings
import base64
import json
logger = logging.getLogger(__name__)

oauth2_scheme = OAuth2PasswordBearer(tokenUrl=settings.API_PREFIX)
SECRET_KEY = settings.JWT_SECRET
ALGORITHM = settings.JWT_ALGORITHM
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
    logger.debug(f"Token received for processing: {token}")

    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )

    missing_claims_exception = HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="Token is missing required claims (sub, userId, companyId).",
    )

    try:
        if settings.AUTH_VALIDATION_ENABLED:
            logger.debug("AUTH_VALIDATION_ENABLED=True: Verifying signature.")
            payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        else:
            logger.debug("AUTH_VALIDATION_ENABLED=False: Skipping signature verification. Using get_unverified_claims.")
            payload = decode_unverified_payload(token)

        logger.debug(f"Decoded payload: {payload}")

        # Now extract user info
        username: Optional[str] = payload.get("sub")
        user_id: Optional[Any] = payload.get("userId")
        company_id_str: Optional[str] = payload.get("companyId")
        role_array: Optional[List[Dict[str, str]]] = payload.get("role")

        if not username or not user_id or not company_id_str:
            logger.warning("Missing critical claims in token.")
            raise missing_claims_exception

        # Parse business_id from companyId string
        parts = company_id_str.split('-')
        if len(parts) >= 3:
            try:
                business_details_id = int(parts[2])
            except ValueError:
                logger.warning(f"Invalid businessId format in companyId: {company_id_str}")
                raise missing_claims_exception
        else:
            logger.warning(f"Invalid companyId format: {company_id_str}")
            raise missing_claims_exception

        roles: List[str] = []
        if isinstance(role_array, list):
            for auth_obj in role_array:
                if isinstance(auth_obj, dict) and "authority" in auth_obj:
                    roles.append(str(auth_obj["authority"]))

        user_data_to_return = {
            "username": username,
            "user_id": user_id,
            "business_id": business_details_id,
            "company_id_str": company_id_str,
            "roles": roles
        }

        logger.debug(f"User data extracted from token: {user_data_to_return}")
        return user_data_to_return

    except JWTError as e:
        logger.error(f"JWTError decoding token: {e}")
        raise credentials_exception
    except Exception as e:
        logger.error(f"Unexpected error while decoding token: {e}", exc_info=True)
        raise credentials_exception
