import pytest
from app.services.validator import validate_brands_csv
from app.models.schemas import BrandValidationResult

@pytest.mark.asyncio
async def test_validate_brands_csv_valid_data():
    content = b"brand_name\nNike\nAdidas\n"
    result = await validate_brands_csv(content)
    assert result.is_valid is True
    assert len(result.errors) == 0

@pytest.mark.asyncio
async def test_validate_brands_csv_empty_file():
    content = b""
    result = await validate_brands_csv(content)
    assert result.is_valid is False
    assert len(result.errors) > 0
    assert any("Missing 'brand_name' header" in error['error'] for error in result.errors)

@pytest.mark.asyncio
async def test_validate_brands_csv_missing_header():
    content = b"name\nNike\n"
    result = await validate_brands_csv(content)
    assert result.is_valid is False
    assert any("Missing 'brand_name' header" in error['error'] for error in result.errors)

@pytest.mark.asyncio
async def test_validate_brands_csv_empty_brand_name():
    content = b"brand_name\nNike\n\nAdidas" # Empty line means empty brand_name
    result = await validate_brands_csv(content)
    assert result.is_valid is False
    assert any(error['field'] == 'brand_name' and "is required" in error['error'] and error['line_number'] == 3 for error in result.errors)

@pytest.mark.asyncio
async def test_validate_brands_csv_duplicate_brand_name():
    content = b"brand_name\nNike\nAdidas\nNike"
    result = await validate_brands_csv(content)
    assert result.is_valid is False
    assert any("not unique" in error['error'] and error['field'] == 'brand_name' and error['line_number'] == 4 for error in result.errors)

@pytest.mark.asyncio
async def test_validate_brands_csv_utf8_error():
    content = b"brand_name\nN\xfaike" # Invalid UTF-8 sequence
    result = await validate_brands_csv(content)
    assert result.is_valid is False
    assert any("File must be UTF-8 encoded" in error['error'] for error in result.errors)

@pytest.mark.asyncio
async def test_validate_brands_csv_completely_invalid_csv_format():
    content = b"brand_name;Nike\nAdidas" # Using semicolon instead of comma, and not proper rows
    result = await validate_brands_csv(content)
    # This might be caught as a generic CSV format error or header error depending on DictReader behavior
    assert result.is_valid is False
    assert len(result.errors) > 0
    # Check if either a header error or a general CSV error is reported
    assert any("Missing 'brand_name' header" in error['error'] or "Invalid CSV format" in error['error'] for error in result.errors)

@pytest.mark.asyncio
async def test_validate_brands_csv_valid_with_extra_columns():
    content = b"brand_name,description\nNike,Sportswear\nAdidas,Footwear"
    result = await validate_brands_csv(content)
    assert result.is_valid is True # Extra columns are ignored by current logic for brands
    assert len(result.errors) == 0

@pytest.mark.asyncio
async def test_validate_brands_csv_header_only():
    content = b"brand_name\n"
    result = await validate_brands_csv(content)
    assert result.is_valid is True # No data rows, so no data errors
    assert len(result.errors) == 0

from app.services.validator import validate_attributes_csv # Add this import
from app.models.schemas import AttributeValidationResult # Add this import

@pytest.mark.asyncio
async def test_validate_attributes_csv_valid_data():
    content = b"attribute_name,allowed_values\nColor,Red|Green|Blue\nSize,S|M|L"
    result = await validate_attributes_csv(content)
    assert result.is_valid is True
    assert len(result.errors) == 0

@pytest.mark.asyncio
async def test_validate_attributes_csv_missing_headers():
    content = b"name,values\nColor,Red|Green"
    result = await validate_attributes_csv(content)
    assert result.is_valid is False
    assert any("Missing required headers" in error['error'] for error in result.errors)

@pytest.mark.asyncio
async def test_validate_attributes_csv_missing_attribute_name_value():
    content = b"attribute_name,allowed_values\n,Red|Green\nSize,"
    result = await validate_attributes_csv(content)
    assert result.is_valid is False
    # Expect error for missing attribute_name on line 2
    assert any(e['line_number'] == 2 and e['field'] == 'attribute_name' and e['error'] == 'is required' for e in result.errors)
    # Expect error for missing allowed_values on line 3
    assert any(e['line_number'] == 3 and e['field'] == 'allowed_values' and e['error'] == 'is required' for e in result.errors)

@pytest.mark.asyncio
async def test_validate_attributes_csv_duplicate_attribute_name():
    content = b"attribute_name,allowed_values\nColor,Red\nColor,Blue"
    result = await validate_attributes_csv(content)
    assert result.is_valid is False
    assert any(e['line_number'] == 3 and e['field'] == 'attribute_name' and "not unique" in e['error'] for e in result.errors)

@pytest.mark.asyncio
async def test_validate_attributes_csv_empty_file():
    content = b""
    result = await validate_attributes_csv(content)
    assert result.is_valid is False
    assert any("Missing required headers" in error['error'] for error in result.errors) # Or similar general CSV error

@pytest.mark.asyncio
async def test_validate_attributes_csv_header_only():
    content = b"attribute_name,allowed_values\n"
    result = await validate_attributes_csv(content)
    assert result.is_valid is True # No data rows means no data errors
    assert len(result.errors) == 0

# Consider adding a test for allowed_values format if specific rules were added to validator,
# e.g. "Red||Blue" (empty value between separators)
# @pytest.mark.asyncio
# async def test_validate_attributes_csv_empty_part_in_allowed_values():
#     content = b"attribute_name,allowed_values\nColor,Red||Blue"
#     result = await validate_attributes_csv(content)
#     assert result.is_valid is False
#     assert any(e['line_number'] == 2 and e['field'] == 'allowed_values' and "contains empty values" in e['error'] for e in result.errors)
