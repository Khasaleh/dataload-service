from fastapi import APIRouter

router = APIRouter()

@router.post("/api/v1/business/{business_id}/upload/{load_type}")
async def upload_file():
    return {"status": "received"}
