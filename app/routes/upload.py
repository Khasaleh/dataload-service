from fastapi import APIRouter, UploadFile, File, Depends, HTTPException
from app.dependencies.auth import get_current_user
from app.services.validator import validate_csv
from app.services.wasabiclient import upload_to_wasabi, delete_from_wasabi
from app.tasks.load_jobs import load_product_data, load_item_data, load_meta_data, load_price_data, load_brand_data, load_attribute_data, load_return_policy_data
import csv
import io

router = APIRouter()

ROLE_PERMISSIONS = {
    "admin": {"brands", "attributes", "return_policies", "products", "product_items", "product_prices", "meta_tags"},
    "catalog_editor": {"products", "product_items", "product_prices", "meta_tags"},
    "viewer": set(),
}

@router.post("/api/v1/business/{business_id}/upload/{load_type}",
             summary="Upload catalog file",
             responses={
                 200: {
                     "description": "Success",
                     "content": {
                         "application/json": {
                             "example": {
                                 "status": "accepted",
                                 "records": 12
                             }
                         }
                     }
                 },
                 422: {
                     "description": "Validation Failed",
                     "content": {
                         "application/json": {
                             "example": {
                                 "status": "error",
                                 "errors": [
                                     {"row": 2, "field": "price", "error": "invalid float"}
                                 ]
                             }
                         }
                     }
                 }
             })
async def upload_file(
    business_id: str,
    load_type: str,
    file: UploadFile = File(...),
    user: dict = Depends(get_current_user)
):
    if business_id != user["business_id"]:
        raise HTTPException(status_code=403, detail="Token does not match business")

    role = user.get("role")
    if role not in ROLE_PERMISSIONS or load_type not in ROLE_PERMISSIONS[role]:
        raise HTTPException(status_code=403, detail="User does not have permission to upload this type")

    if load_type not in {"brands", "attributes", "return_policies", "products", "product_items", "product_prices", "meta_tags"}:
        raise HTTPException(status_code=400, detail="Invalid load type")

    # Upload to Wasabi first
    s3_key = f"{business_id}/{load_type}.csv"
    content = await file.read()
    upload_to_wasabi(s3_key, content)

    decoded = content.decode("utf-8")
    csv_reader = csv.DictReader(io.StringIO(decoded))
    records = list(csv_reader)
    if not records:
        delete_from_wasabi(s3_key)
        raise HTTPException(status_code=400, detail="Empty CSV file")

    errors, valid_rows = validate_csv(load_type, records)
    if errors:
        delete_from_wasabi(s3_key)
        return {"status": "error", "errors": errors}

    try:
        for record in valid_rows:
            if load_type == "products":
                load_product_data.delay(business_id, record)
            elif load_type == "product_items":
                load_item_data.delay(business_id, record)
            elif load_type == "meta_tags":
                load_meta_data.delay(business_id, record)
            elif load_type == "product_prices":
                load_price_data.delay(business_id, record)
            elif load_type == "brands":
                load_brand_data.delay(business_id, record)
            elif load_type == "attributes":
                load_attribute_data.delay(business_id, record)
            elif load_type == "return_policies":
                load_return_policy_data.delay(business_id, record)
    except Exception:
        delete_from_wasabi(s3_key)
        raise HTTPException(status_code=500, detail="Task enqueue failed")

    return {"status": "accepted", "records": len(valid_rows)}
