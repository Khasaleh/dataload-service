import pytest
from httpx import AsyncClient
from fastapi import FastAPI
from unittest.mock import patch, MagicMock # For mocking Celery task

# Need to make sure the app instance used by AsyncClient is the one with routes
from app.main import app as main_app # Import your FastAPI app instance
# from app.models.schemas import BrandValidationResult # This model no longer exists

@pytest.fixture
def app() -> FastAPI:
    return main_app

@pytest.mark.asyncio
async def test_upload_brands_file_success(app: FastAPI):
    # Mock the Celery task's delay method
    mock_task = MagicMock()
    mock_task.id = "test_task_id" # Simulate task_id attribute

    with patch('app.routes.upload.process_brands_data.delay', return_value=mock_task) as mock_delay:
        csv_content = b"brand_name\nTestBrand1\nTestBrand2"
        files = {'file': ('brands.csv', csv_content, 'text/csv')}

        async with AsyncClient(app=app, base_url="http://test") as client:
            response = await client.post("/api/v1/business/1/upload/brands", files=files)

        assert response.status_code == 202 # Accepted
        response_data = response.json()
        assert response_data['message'] == "Brand file accepted for processing."
        assert response_data['business_id'] == 1
        assert response_data['task_id'] == "test_task_id"
        mock_delay.assert_called_once_with(1, csv_content.decode('utf-8'))

@pytest.mark.asyncio
async def test_upload_brands_file_validation_error(app: FastAPI):
    # No need to mock Celery here as validation should fail before task dispatch
    csv_content = b"brand_name\nTestBrand1\nTestBrand1" # Duplicate brand name
    files = {'file': ('brands.csv', csv_content, 'text/csv')}

    async with AsyncClient(app=app, base_url="http://test") as client:
        response = await client.post("/api/v1/business/1/upload/brands", files=files)

    assert response.status_code == 422 # Unprocessable Entity for validation errors
    response_data = response.json()
    assert 'detail' in response_data
    assert isinstance(response_data['detail'], list)
    assert len(response_data['detail']) > 0
    assert any("not unique" in error['error'] for error in response_data['detail'])

@pytest.mark.asyncio
async def test_upload_brands_file_invalid_file_type(app: FastAPI):
    files = {'file': ('brands.txt', b"some text data", 'text/plain')}

    async with AsyncClient(app=app, base_url="http://test") as client:
        response = await client.post("/api/v1/business/1/upload/brands", files=files)

    assert response.status_code == 400 # Bad Request for wrong file type
    response_data = response.json()
    assert response_data['detail'] == "Invalid file type. Only CSV is allowed."

@pytest.mark.asyncio
async def test_upload_brands_file_bad_csv_header(app: FastAPI):
    csv_content = b"wrong_header\nBrandX"
    files = {'file': ('brands.csv', csv_content, 'text/csv')}

    async with AsyncClient(app=app, base_url="http://test") as client:
        response = await client.post("/api/v1/business/1/upload/brands", files=files)

    assert response.status_code == 422 # Validation error
    response_data = response.json()
    assert any("Missing 'brand_name' header" in error['error'] for error in response_data['detail'])


@pytest.mark.asyncio
async def test_upload_attributes_file_success(app: FastAPI):
    mock_task = MagicMock()
    mock_task.id = "test_attr_task_id"
    with patch('app.routes.upload.process_attributes_data.delay', return_value=mock_task) as mock_delay:
        csv_content = b"attribute_name,allowed_values\nColor,Red|Blue\nSize,S|M"
        files = {'file': ('attributes.csv', csv_content, 'text/csv')}

        async with AsyncClient(app=app, base_url="http://test") as client:
            response = await client.post("/api/v1/business/123/upload/attributes", files=files)

        assert response.status_code == 202
        response_data = response.json()
        assert response_data['message'] == "Attribute file accepted for processing."
        assert response_data['business_id'] == 123
        assert response_data['task_id'] == "test_attr_task_id"
        mock_delay.assert_called_once_with(123, csv_content.decode('utf-8'))

@pytest.mark.asyncio
async def test_upload_attributes_file_validation_error(app: FastAPI):
    csv_content = b"attribute_name,allowed_values\nColor,Red\nColor,Blue" # Duplicate name
    files = {'file': ('attributes.csv', csv_content, 'text/csv')}

    async with AsyncClient(app=app, base_url="http://test") as client:
        response = await client.post("/api/v1/business/1/upload/attributes", files=files)

    assert response.status_code == 422
    response_data = response.json()
    assert 'detail' in response_data
    assert any("not unique" in error['error'] for error in response_data['detail'])

@pytest.mark.asyncio
async def test_upload_attributes_file_missing_header_route(app: FastAPI):
    csv_content = b"attr_name\nColor" # Missing allowed_values header
    files = {'file': ('attributes.csv', csv_content, 'text/csv')}

    async with AsyncClient(app=app, base_url="http://test") as client:
        response = await client.post("/api/v1/business/1/upload/attributes", files=files)

    assert response.status_code == 422 # Validation error for headers
    response_data = response.json()
    assert 'detail' in response_data
    assert any("Missing required headers" in error['error'] for error in response_data['detail'])
