from pydantic import BaseModel

class TokenData(BaseModel):
    business_id: str | None = None
    role: str
    exp: datetime