import pytest
from app.services.validator import validate_csv # Use the generic validator
# BrandValidationResult and AttributeValidationResult are no longer used

# Helper to simulate CSV parsing for tests, as validate_csv expects list of dicts
def records_from_content_string(content_str: str) -> list[dict]:
    import csv
    import io
    if not content_str.strip(): # Handle empty content string
        return []
    lines = content_str.strip().split('\n')
    if len(lines) == 0:
        return []
    if len(lines) == 1 and not lines[0].strip(): # Only whitespace
        return []
    try:
        return list(csv.DictReader(io.StringIO(content_str)))
    except Exception:
        return [{"__parse_error__": "Invalid CSV format"}]


@pytest.mark.asyncio
async def test_validate_brands_csv_valid_data(): # Test name kept for clarity
    content = "name,logo\nNike,nike.png\nAdidas,adidas.png"
    records = records_from_content_string(content)

    errors, valid_rows = validate_csv(
        load_type="brands",
        records=records,
        session_id="test_session_brands_valid", # Mock session_id
        record_key="name" # 'name' is the key in BrandCsvModel
    )
    assert not errors
    assert len(valid_rows) == 2
    assert valid_rows[0]['name'] == 'Nike'
    assert valid_rows[1]['logo'] == 'adidas.png'


@pytest.mark.asyncio
async def test_validate_brands_csv_empty_file():
    records = []
    errors, valid_rows = validate_csv(load_type="brands", records=records)
    assert not errors
    assert not valid_rows

@pytest.mark.asyncio
async def test_validate_brands_csv_missing_header():
    records = [{"wrong_header": "Nike"}]
    errors, valid_rows = validate_csv(load_type="brands", records=records)
    assert len(errors) > 0
    assert any(err['field'] == 'name' and 'Field required' in err['error'] for err in errors)
    assert any(err['field'] == 'logo' and 'Field required' in err['error'] for err in errors)


@pytest.mark.asyncio
async def test_validate_brands_csv_empty_brand_name():
    records = [
        {"name": "Nike", "logo": "n.png"},
        {"name": "", "logo": "empty.png"},
        {"name": "Adidas", "logo": "a.png"}
    ]
    errors, valid_rows = validate_csv(load_type="brands", records=records)
    assert len(errors) > 0
    assert any(err['row'] == 2 and err['field'] == 'name' and "String should have at least 1 character" in err['error'] for err in errors)

@pytest.mark.asyncio
async def test_validate_brands_csv_duplicate_brand_name():
    records = [
        {"name": "Nike", "logo": "n1.png"},
        {"name": "Adidas", "logo": "a.png"},
        {"name": "Nike", "logo": "n2.png"}
    ]
    errors, valid_rows = validate_csv(
        load_type="brands",
        records=records,
        session_id="test_session_brands_dup",
        record_key="name"
    )
    assert len(errors) > 0
    assert any(err['error'] == "Duplicate key found in file" and err['field'] == 'name' and err['key'] == 'Nike' for err in errors)


@pytest.mark.asyncio
async def test_validate_brands_csv_utf8_error():
    pytest.skip("UTF-8 decoding errors are handled before validate_csv typically.")


@pytest.mark.asyncio
async def test_validate_brands_csv_completely_invalid_csv_format():
    records_malformed = [ "this is not a dict" ] # type: ignore
    errors, _ = validate_csv(load_type="brands", records=records_malformed)
    assert len(errors) > 0
    assert any("Input should be a valid dictionary" in e['error'] for e in errors)


@pytest.mark.asyncio
async def test_validate_brands_csv_valid_with_extra_columns():
    records = [
        {"name": "Nike", "logo": "n.png", "description": "Sportswear"},
        {"name": "Adidas", "logo": "a.png", "description": "Footwear"}
    ]
    errors, valid_rows = validate_csv(load_type="brands", records=records)
    assert not errors
    assert len(valid_rows) == 2
    assert valid_rows[0]['name'] == 'Nike'


@pytest.mark.asyncio
async def test_validate_brands_csv_header_only():
    records = []
    errors, valid_rows = validate_csv(load_type="brands", records=records)
    assert not errors
    assert not valid_rows


@pytest.mark.asyncio
async def test_validate_attributes_csv_valid_data():
    records = [
        {"attribute_name": "Color", "is_color": True, "values_name": "Red|Green|Blue", "value_value": "#FF0000|#00FF00|#0000FF"},
        {"attribute_name": "Size", "is_color": False, "values_name": "S|M|L"}
    ]
    errors, valid_rows = validate_csv(
        load_type="attributes",
        records=records,
        session_id="test_session_attr_valid",
        record_key="attribute_name"
    )
    assert not errors
    assert len(valid_rows) == 2
    assert valid_rows[0]['attribute_name'] == 'Color'

@pytest.mark.asyncio
async def test_validate_attributes_csv_missing_headers():
    records = [{"name": "Color", "values": "Red|Green"}]
    errors, _ = validate_csv(load_type="attributes", records=records)
    assert len(errors) > 0
    assert any(e['field'] == 'attribute_name' and 'Field required' in e['error'] for e in errors)
    assert any(e['field'] == 'is_color' and 'Field required' in e['error'] for e in errors)


@pytest.mark.asyncio
async def test_validate_attributes_csv_missing_attribute_name_value():
    records_trigger = [
        {"attribute_name": "Size", "is_color": False, "values_name": None, "value_value": "S|M|L"}
    ]
    errors_trigger, _ = validate_csv(load_type="attributes", records=records_trigger)
    assert any("'values_name' must be provided if 'value_value', 'img_url', or 'values_active' are specified." in e['error'] for e in errors_trigger)

@pytest.mark.asyncio
async def test_validate_attributes_csv_duplicate_attribute_name():
    records = [
        {"attribute_name": "Color", "is_color": True, "values_name": "Red"},
        {"attribute_name": "Color", "is_color": False, "values_name": "Blue"}
    ]
    errors, _ = validate_csv(
        load_type="attributes",
        records=records,
        session_id="test_session_attr_dup",
        record_key="attribute_name"
    )
    assert len(errors) > 0
    assert any(e['error'] == "Duplicate key found in file" and e['field'] == 'attribute_name' and e['key'] == 'Color' for e in errors)


@pytest.mark.asyncio
async def test_validate_attributes_csv_empty_file():
    records = []
    errors, valid_rows = validate_csv(load_type="attributes", records=records)
    assert not errors
    assert not valid_rows

@pytest.mark.asyncio
async def test_validate_attributes_csv_header_only():
    records = []
    errors, valid_rows = validate_csv(load_type="attributes", records=records)
    assert not errors
    assert not valid_rows
