from fastapi import FastAPI
from app.routes import upload as upload_router # Changed here

app = FastAPI(
    title="Data Upload Service API",
    description="API for uploading and processing business catalog data.",
    version="0.1.0"
)

@app.get("/ping")
async def ping():
    return {"message": "pong"}

app.include_router(upload_router.router, prefix="/api/v1") # Changed here
