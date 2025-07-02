from fastapi import APIRouter, Depends, HTTPException
from typing import Any

from app.dependencies.auth import get_current_user
from app.models.schemas import UserResponseSchema

router = APIRouter(
    prefix="/users", # This prefix will result in /api/v1/users
    tags=["Users"]
)

@router.get("/me", response_model=UserResponseSchema)
async def read_users_me(current_user: dict = Depends(get_current_user)):
    """
    Get current authenticated user's details.
    """
    # The current_user dict from get_current_user already contains:
    # "username": str
    # "user_id": Any (typically int or str)
    # "business_id": int (this is the parsed integer business_details_id)
    # "company_id_str": str (original companyId string from token)
    # "roles": List[str]
    # This structure matches UserResponseSchema if field names are aligned.
    # Pydantic will automatically map if field names are the same.

    # Ensure all fields expected by UserResponseSchema are present in current_user
    # or handle missing ones if necessary (though get_current_user should provide them)
    if not all(key in current_user for key in ["user_id", "username", "business_id", "roles"]):
        # This case should ideally be caught by get_current_user raising an exception
        # if essential claims are missing.
        raise HTTPException(status_code=500, detail="User context is missing essential information.")

    return UserResponseSchema(**current_user)
    # Alternative, if Pydantic model directly matches the dict keys from get_current_user:
    # return current_user
    # However, explicitly constructing with UserResponseSchema(**current_user)
    # provides better validation and clarity.
    # UserResponseSchema has fields: user_id, username, business_id, roles, company_id_str (optional)
    # get_current_user returns: user_id, username, business_id, roles, company_id_str
    # So direct construction `UserResponseSchema(**current_user)` should work.
