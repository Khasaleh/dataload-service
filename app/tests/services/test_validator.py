import pytest
from unittest.mock import patch, MagicMock

from app.services.validator import (
    check_file_uniqueness,
    check_referential_integrity,
    validate_csv,
    MODEL_MAP # For testing validate_csv with different load_types
)
from app.models.schemas import BrandModel # Example model for testing validate_csv

# Sample data for testing
SAMPLE_RECORDS_UNIQUE = [
    {"sku": "SKU001", "name": "Product A"},
    {"sku": "SKU002", "name": "Product B"},
    {"sku": "SKU003", "name": "Product C"},
]

SAMPLE_RECORDS_DUPLICATE = [
    {"sku": "SKU001", "name": "Product A"},
    {"sku": "SKU002", "name": "Product B"},
    {"sku": "SKU001", "name": "Product C"}, # Duplicate SKU001
    {"sku": "SKU003", "name": "Product D"},
    {"sku": "SKU002", "name": "Product E"}, # Duplicate SKU002
]

SAMPLE_RECORDS_FOR_REF_INTEGRITY = [
    {"id": 1, "brand_name": "BrandX"},
    {"id": 2, "brand_name": "BrandY"},
    {"id": 3, "brand_name": "BrandZ"}, # This one might not exist in mock Redis
]


class TestCheckFileUniqueness:
    def test_no_duplicates(self):
        errors = check_file_uniqueness(SAMPLE_RECORDS_UNIQUE, "sku")
        assert len(errors) == 0

    def test_with_duplicates(self):
        errors = check_file_uniqueness(SAMPLE_RECORDS_DUPLICATE, "sku")
        assert len(errors) == 2 # SKU001 and SKU002 are duplicated

        sku001_error = next(e for e in errors if e["key"] == "SKU001")
        sku002_error = next(e for e in errors if e["key"] == "SKU002")

        assert sku001_error["error"] == "Duplicate key found in file"
        assert sku001_error["rows"] == [1, 3] # 1-based indexing
        assert sku001_error["field"] == "sku"

        assert sku002_error["error"] == "Duplicate key found in file"
        assert sku002_error["rows"] == [2, 5] # 1-based indexing
        assert sku002_error["field"] == "sku"

    def test_key_not_present(self):
        records = [{"id": 1}, {"id": 2}]
        errors = check_file_uniqueness(records, "sku")
        assert len(errors) == 0

    def test_empty_records(self):
        errors = check_file_uniqueness([], "sku")
        assert len(errors) == 0


class TestCheckReferentialIntegrity:
    @patch('app.services.validator.get_from_id_map')
    def test_all_references_exist(self, mock_get_from_id_map):
        # Mock get_from_id_map to return a dummy value (truthy) for existing keys
        mock_get_from_id_map.side_effect = lambda session_id, entity_type, key_value: "some_id" if key_value in ["BrandX", "BrandY"] else None

        errors = check_referential_integrity(
            records=SAMPLE_RECORDS_FOR_REF_INTEGRITY,
            field_to_check="brand_name",
            referenced_entity_type="brand",
            session_id="test_session_123"
        )
        # Expecting one error for BrandZ
        assert len(errors) == 1
        assert errors[0]["value"] == "BrandZ"
        assert errors[0]["row"] == 3
        assert errors[0]["field"] == "brand_name"
        assert "Referenced brand not found" in errors[0]["error"]

        # Check calls to mock
        assert mock_get_from_id_map.call_count == 3
        mock_get_from_id_map.assert_any_call("test_session_123", "brand", "BrandX")
        mock_get_from_id_map.assert_any_call("test_session_123", "brand", "BrandY")
        mock_get_from_id_map.assert_any_call("test_session_123", "brand", "BrandZ")


    @patch('app.services.validator.get_from_id_map')
    def test_some_references_missing(self, mock_get_from_id_map):
        # Mock get_from_id_map to simulate BrandY and BrandZ missing
        mock_get_from_id_map.side_effect = lambda session_id, entity_type, key_value: "some_id" if key_value == "BrandX" else None

        errors = check_referential_integrity(
            records=SAMPLE_RECORDS_FOR_REF_INTEGRITY,
            field_to_check="brand_name",
            referenced_entity_type="brand",
            session_id="test_session_456"
        )
        assert len(errors) == 2
        assert errors[0]["value"] == "BrandY"
        assert errors[0]["row"] == 2
        assert errors[1]["value"] == "BrandZ"
        assert errors[1]["row"] == 3

    @patch('app.services.validator.get_from_id_map')
    def test_field_not_in_records(self, mock_get_from_id_map):
        records = [{"id": 1, "product_code": "P1"}, {"id": 2, "product_code": "P2"}]
        errors = check_referential_integrity(records, "brand_name", "brand", "test_session")
        assert len(errors) == 0
        mock_get_from_id_map.assert_not_called()

    @patch('app.services.validator.get_from_id_map')
    def test_empty_records_for_ref_integrity(self, mock_get_from_id_map):
        errors = check_referential_integrity([], "brand_name", "brand", "test_session")
        assert len(errors) == 0
        mock_get_from_id_map.assert_not_called()


class TestValidateCsv:
    # Using 'brands' load_type and BrandModel for simplicity in these tests
    # BrandModel only has 'brand_name' which must not be empty.

    def test_valid_data_all_checks_pass(self):
        records = [{"brand_name": "Brand A"}, {"brand_name": "Brand B"}]
        # No uniqueness check, no referential integrity for this simple case
        errors, valid_rows = validate_csv(
            load_type="brands",
            records=records,
            session_id="s1",
            record_key=None, # no uniqueness for this test
            referenced_entity_map=None # no ref integrity for this test
        )
        assert len(errors) == 0
        assert len(valid_rows) == 2
        assert valid_rows[0]["brand_name"] == "Brand A"

    def test_pydantic_validation_errors(self):
        records = [{"brand_name": "Brand A"}, {"brand_name": " "}, {"brand_name": "Brand C"}]
        errors, valid_rows = validate_csv("brands", records) # Using default args for others

        assert len(errors) == 1
        assert errors[0]["row"] == 2
        assert errors[0]["field"] == "brand_name"
        assert "empty" in errors[0]["error"]

        # valid_rows should contain only the rows that passed Pydantic validation
        assert len(valid_rows) == 2
        assert valid_rows[0]["brand_name"] == "Brand A"
        assert valid_rows[1]["brand_name"] == "Brand C"

    @patch('app.services.validator.check_file_uniqueness')
    def test_uniqueness_errors(self, mock_check_file_uniqueness):
        records = [{"brand_name": "Brand A"}, {"brand_name": "Brand A"}] # Pydantic valid
        mock_check_file_uniqueness.return_value = [{
            "error": "Duplicate key found in file", "key": "Brand A", "rows": [1, 2], "field": "brand_name"
        }]

        errors, valid_rows = validate_csv(
            load_type="brands",
            records=records,
            record_key="brand_name" # Enable uniqueness check
        )

        assert len(errors) == 1
        assert errors[0]["error"] == "Duplicate key found in file"
        mock_check_file_uniqueness.assert_called_once_with(records, "brand_name")
        # Pydantic validation passed for both, so valid_rows contains both
        assert len(valid_rows) == 2


    @patch('app.services.validator.check_referential_integrity')
    def test_referential_integrity_errors(self, mock_check_referential_integrity):
        # Assume ProductModel for this test, which can have brand_name reference
        product_records = [
            {"product_name": "P1", "brand_name": "MissingBrand", "product_url": "u", "category_path": "c", "return_policy_code": "r", "package_length":1, "package_width":1, "package_height":1, "package_weight":1, "status":"s"}
        ]
        mock_check_referential_integrity.return_value = [{
            "row": 1, "field": "brand_name", "error": "Referenced brand not found", "value": "MissingBrand"
        }]

        referenced_map = {'brand_name': 'brand'}

        errors, valid_rows = validate_csv(
            load_type="products", # Use a model that has referential checks configured in real scenario
            records=product_records,
            session_id="s1",
            referenced_entity_map=referenced_map
        )

        assert len(errors) == 1
        assert errors[0]["error"] == "Referenced brand not found"
        mock_check_referential_integrity.assert_called_once_with(
            product_records, "brand_name", "brand", "s1"
        )
        assert len(valid_rows) == 1 # Pydantic validation passed

    def test_unsupported_load_type(self):
        records = [{"data": "test"}]
        errors, valid_rows = validate_csv("unknown_type", records)
        assert len(errors) == 1
        assert errors[0]["error"] == "Unsupported load type: unknown_type"
        assert len(valid_rows) == 0

    @patch('app.services.validator.check_referential_integrity')
    @patch('app.services.validator.check_file_uniqueness')
    def test_combination_of_errors(self, mock_check_file_uniqueness, mock_check_referential_integrity):
        # Pydantic error, uniqueness error, and ref integrity error
        # For this, we need a slightly more complex setup. Let's use 'products'
        # ProductModel: product_name, brand_name, etc.

        records_with_issues = [
            {"product_name": " ", "brand_name": "BrandA", "product_url":"u","category_path":"c","return_policy_code":"r","package_length":1,"package_width":1,"package_height":1,"package_weight":1,"status":"s"}, # Pydantic error on product_name
            {"product_name": "Prod1", "brand_name": "BrandA", "product_url":"u","category_path":"c","return_policy_code":"r","package_length":1,"package_width":1,"package_height":1,"package_weight":1,"status":"s"}, # Potentially unique error if Prod1 is repeated for product_name
            {"product_name": "Prod2", "brand_name": "MissingBrand", "product_url":"u","category_path":"c","return_policy_code":"r","package_length":1,"package_width":1,"package_height":1,"package_weight":1,"status":"s"} # Ref error on MissingBrand
        ]

        # Mock uniqueness check to return an error if called
        # Uniqueness check is only called if there are no Pydantic errors.
        # So, for this combined test, we'll assume Pydantic errors dominate.
        # If Pydantic errors exist, other checks (uniqueness, ref integrity) are currently skipped by the logic in validate_csv.
        # Let's test that behavior first.

        errors, valid_rows = validate_csv(
            load_type="products",
            records=records_with_issues,
            session_id="s1",
            record_key="product_name",
            referenced_entity_map={'brand_name': 'brand'}
        )

        assert any(e['field'] == 'product_name' and 'empty' in e['error'] for e in errors) # Pydantic error
        assert len(valid_rows) == 2 # Only Prod1 and Prod2 pass Pydantic

        # Since there was a Pydantic error, these should not have been called by current logic in validator.py
        mock_check_file_uniqueness.assert_not_called()
        mock_check_referential_integrity.assert_not_called()

        # Now, let's test a scenario where Pydantic passes, but others fail.
        records_pydantic_ok = [
            {"product_name": "Prod1", "brand_name": "BrandA", "product_url":"u1","category_path":"c1","return_policy_code":"r1","package_length":1,"package_width":1,"package_height":1,"package_weight":1,"status":"s1"},
            {"product_name": "Prod1", "brand_name": "BrandB", "product_url":"u2","category_path":"c2","return_policy_code":"r2","package_length":1,"package_width":1,"package_height":1,"package_weight":1,"status":"s2"}, # Duplicate product_name for uniqueness check
            {"product_name": "Prod2", "brand_name": "MissingBrand", "product_url":"u3","category_path":"c3","return_policy_code":"r3","package_length":1,"package_width":1,"package_height":1,"package_weight":1,"status":"s3"} # MissingBrand for ref check
        ]

        mock_check_file_uniqueness.return_value = [{"error": "Duplicate product_name", "key": "Prod1", "rows": [1,2], "field": "product_name"}]
        mock_check_referential_integrity.return_value = [{"error": "Missing brand", "value": "MissingBrand", "row": 3, "field": "brand_name"}]

        errors_no_pydantic, valid_rows_no_pydantic = validate_csv(
            load_type="products",
            records=records_pydantic_ok,
            session_id="s2",
            record_key="product_name",
            referenced_entity_map={'brand_name': 'brand'}
        )

        assert len(errors_no_pydantic) == 2 # Uniqueness and Referential
        assert any("Duplicate product_name" in e['error'] for e in errors_no_pydantic)
        assert any("Missing brand" in e['error'] for e in errors_no_pydantic)

        mock_check_file_uniqueness.assert_called_once_with(records_pydantic_ok, "product_name")
        mock_check_referential_integrity.assert_called_once_with(records_pydantic_ok, "brand_name", "brand", "s2")

        assert len(valid_rows_no_pydantic) == 3 # All passed Pydantic initially
```
