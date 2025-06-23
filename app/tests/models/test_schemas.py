import pytest
from pydantic import ValidationError
from datetime import datetime

from app.models.schemas import (
    BrandCsvModel,
    AttributeCsvModel,
    ReturnPolicyCsvModel,
    ProductModel,
    ProductItemModel,
    ProductPriceModel,
    MetaTagModel,
    UploadSessionModel
)

# General utility for checking non-empty string validation
def check_non_empty_validator(model_class, field_name, valid_value="test", invalid_value=" ", **kwargs):
    # Test valid value
    all_valid_args = {field_name: valid_value, **kwargs}
    model_instance = model_class(**all_valid_args)
    assert getattr(model_instance, field_name) == valid_value

    # Test invalid value (empty or whitespace)
    all_invalid_args = {field_name: invalid_value, **kwargs}
    with pytest.raises(ValidationError) as excinfo:
        model_class(**all_invalid_args)
    assert field_name in str(excinfo.value).lower()
    # Pydantic v2 constr(min_length=1) error is "String should have at least 1 character"
    assert "string should have at least 1 character" in str(excinfo.value).lower() or "empty" in str(excinfo.value).lower()

# General utility for checking positive number validation
def check_positive_number_validator(model_class, field_name, valid_value=10, invalid_value=0, **kwargs):
    # Test valid value
    all_valid_args = {field_name: valid_value, **kwargs}
    model_instance = model_class(**all_valid_args)
    assert getattr(model_instance, field_name) == valid_value

    # Test invalid value (zero or negative)
    all_invalid_args = {field_name: invalid_value, **kwargs}
    with pytest.raises(ValidationError) as excinfo:
        model_class(**all_invalid_args)
    assert field_name in str(excinfo.value).lower()
    if invalid_value <=0: # specific message for positive
         # Pydantic v2: "Input should be strictly greater than 0"
         assert "greater than 0" in str(excinfo.value).lower() or "positive" in str(excinfo.value).lower()

# General utility for checking non-negative number validation
def check_non_negative_validator(model_class, field_name, valid_value=0, invalid_value=-1, **kwargs):
    all_valid_args = {field_name: valid_value, **kwargs}
    model_instance = model_class(**all_valid_args)
    assert getattr(model_instance, field_name) == valid_value

    all_invalid_args = {field_name: invalid_value, **kwargs}
    with pytest.raises(ValidationError) as excinfo:
        model_class(**all_invalid_args)
    assert field_name in str(excinfo.value).lower()
    # Pydantic v2: "Input should be greater than or equal to 0"
    assert "greater than or equal to 0" in str(excinfo.value).lower() or "non-negative" in str(excinfo.value).lower()


class TestBrandModel:
    def test_brand_name_validation(self):
        # BrandCsvModel requires 'name' and 'logo'.
        # The helper check_non_empty_validator will be updated to pass kwargs.
        check_non_empty_validator(BrandCsvModel, "name", valid_value="Valid Brand Name", logo="logo.png")

class TestAttributeModel:
    def test_attribute_name_validation(self):
        check_non_empty_validator(AttributeCsvModel, "attribute_name", valid_value="Color") # Renamed model

    def test_allowed_values_validation(self):
        # For AttributeCsvModel, 'values_name' and other pipe-separated fields have complex validation.
        # This old test for 'allowed_values' is no longer applicable as 'allowed_values' field was removed.
        # New tests for AttributeCsvModel's root validators would be needed if not already present.
        # For now, just commenting out or removing this specific test.
        # check_non_empty_validator(AttributeCsvModel, "allowed_values", valid_value="Red,Blue")
        pass

class TestReturnPolicyCsvModel: # Renamed class
    # Old tests removed as the model structure and validators have changed significantly.

    def test_return_policy_csv_sales_return_allowed_valid(self):
        data = {
            "return_policy_type": "SALES_RETURN_ALLOWED",
            "time_period_return": 14,
            "policy_name": "14 Day Returns",
        }
        model = ReturnPolicyCsvModel(**data)
        assert model.return_policy_type == "SALES_RETURN_ALLOWED"
        assert model.time_period_return == 14
        assert model.policy_name == "14 Day Returns"

    def test_return_policy_csv_sales_return_allowed_missing_time_period(self):
        data = {
            "return_policy_type": "SALES_RETURN_ALLOWED",
            "policy_name": "Test Policy",
        }
        with pytest.raises(ValidationError) as excinfo:
            ReturnPolicyCsvModel(**data)

        # Check Pydantic v1 style:
        # assert any(
        #     "'time_period_return' is required when 'return_policy_type' is 'SALES_RETURN_ALLOWED'" in str(e.exc)
        #     for e in excinfo.value.raw_errors
        # )
        # Check Pydantic v2 style (more robust):
        errors = excinfo.value.errors()
        assert len(errors) == 1
        assert errors[0]['type'] == 'value_error' # Generic type for root_validator errors often
        # The exact message might be part of context or a custom code in Pydantic v2
        # For now, checking if the specific message is present in the error details
        assert "'time_period_return' is required when 'return_policy_type' is 'SALES_RETURN_ALLOWED'." in str(excinfo.value)


    def test_return_policy_csv_sales_return_allowed_policy_name_optional(self):
        data = {
            "return_policy_type": "SALES_RETURN_ALLOWED",
            "time_period_return": 7,
            # policy_name is missing
        }
        model = ReturnPolicyCsvModel(**data)
        assert model.policy_name is None
        assert model.time_period_return == 7

    def test_return_policy_csv_sales_are_final_valid_with_nulls(self):
        data = {"return_policy_type": "SALES_ARE_FINAL"}
        model = ReturnPolicyCsvModel(**data)
        assert model.return_policy_type == "SALES_ARE_FINAL"
        assert model.time_period_return is None
        assert model.policy_name is None
        assert model.grace_period_return is None

    def test_return_policy_csv_sales_are_final_valid_with_disregarded_values(self):
        # The Pydantic model itself allows these values; the loader service is responsible for nullifying them.
        data = {
            "return_policy_type": "SALES_ARE_FINAL",
            "time_period_return": 10,
            "policy_name": "This policy name will be ignored by loader",
            "grace_period_return": 5
        }
        model = ReturnPolicyCsvModel(**data)
        assert model.time_period_return == 10
        assert model.policy_name == "This policy name will be ignored by loader"
        assert model.grace_period_return == 5


class TestProductModel:
    def test_text_fields_validation(self):
        fields = ['product_name', 'product_url', 'brand_name', 'category_path', 'return_policy_code', 'status']
        # Need to provide all required fields for ProductModel
        base_data = {
            'product_name': 'Test Product', 'product_url': 'http://example.com/product',
            'brand_name': 'TestBrand', 'category_path': 'Test > Category',
            'return_policy_code': 'RP123', 'status': 'active',
            'package_length': 1.0, 'package_width': 1.0,
            'package_height': 1.0, 'package_weight': 1.0
        }
        for field in fields:
            data = base_data.copy()
            data[field] = "Valid Value"
            ProductModel(**data) # Valid

            data[field] = " "
            with pytest.raises(ValidationError):
                ProductModel(**data) # Invalid

    def test_package_dimensions_validation(self):
        fields = ['package_length', 'package_width', 'package_height', 'package_weight']
        base_data = {
            'product_name': 'Test Product', 'product_url': 'http://example.com/product',
            'brand_name': 'TestBrand', 'category_path': 'Test > Category',
            'return_policy_code': 'RP123', 'status': 'active',
            'package_length': 1.0, 'package_width': 1.0,
            'package_height': 1.0, 'package_weight': 1.0
        }
        for field in fields:
            data = base_data.copy()

            data[field] = 0 # Invalid
            with pytest.raises(ValidationError):
                ProductModel(**data)

            data[field] = -1.0 # Invalid
            with pytest.raises(ValidationError):
                ProductModel(**data)

            data[field] = 10.5 # Valid
            ProductModel(**data)


class TestProductItemModel:
    def test_text_fields_validation(self):
        fields = ['product_name', 'variant_sku', 'attribute_combination', 'status', 'published', 'default_sku']
        base_data = {
            'product_name': 'Test Item', 'variant_sku': 'SKU123',
            'attribute_combination': 'Color=Red,Size=M', 'status': 'active',
            'published': 'Yes', 'default_sku': 'SKU123-D', 'quantity': 10
        }
        for field in fields:
            data = base_data.copy()
            data[field] = "Valid"
            ProductItemModel(**data) # Valid

            data[field] = " "
            with pytest.raises(ValidationError):
                ProductItemModel(**data) # Invalid

    def test_quantity_validation(self):
        base_data = {
            'product_name': 'Test Item', 'variant_sku': 'SKU123',
            'attribute_combination': 'Color=Red,Size=M', 'status': 'active',
            'published': 'Yes', 'default_sku': 'SKU123-D', 'quantity': 10
        }
        check_non_negative_validator(ProductItemModel, "quantity", valid_value=0, invalid_value=-5, **base_data)


class TestProductPriceModel:
    def test_product_name_validation(self):
        base_data = {'product_name': 'Test Price Prod', 'price': 10.0, 'cost_per_item': 5.0}
        check_non_empty_validator(ProductPriceModel, "product_name", **base_data)

    def test_price_cost_validation(self):
        base_data = {'product_name': 'Test Price Prod', 'price': 10.0, 'cost_per_item': 5.0}
        check_positive_number_validator(ProductPriceModel, "price", **base_data)
        check_positive_number_validator(ProductPriceModel, "cost_per_item", **base_data)

    def test_offer_price_validation(self):
        # offer_price is Optional, but if present, must be positive
        ProductPriceModel(product_name="p", price=1, cost_per_item=1, offer_price=None)
        ProductPriceModel(product_name="p", price=1, cost_per_item=1, offer_price=0.5)
        with pytest.raises(ValidationError) as excinfo:
            ProductPriceModel(product_name="p", price=1, cost_per_item=1, offer_price=0)
        assert "offer_price" in str(excinfo.value)
        assert "positive" in str(excinfo.value)
        with pytest.raises(ValidationError) as excinfo:
            ProductPriceModel(product_name="p", price=1, cost_per_item=1, offer_price=-10)
        assert "offer_price" in str(excinfo.value)
        assert "positive" in str(excinfo.value)


class TestMetaTagModel:
    def test_product_name_validation(self):
        base_data = {'product_name': 'Test Meta Prod'} # other fields are optional
        check_non_empty_validator(MetaTagModel, "product_name", **base_data)


class TestUploadSessionModel:
    def test_valid_load_types(self):
        valid_types = ["brands", "attributes", "return_policies", "products", "product_items", "product_prices", "meta_tags"]
        base_data = {
            'business_id': 'biz123', 'original_filename': 'file.csv',
            'wasabi_path': 'path/to/file.csv'
        }
        for load_type in valid_types:
            data = base_data.copy()
            data['load_type'] = load_type
            session = UploadSessionModel(**data)
            assert session.load_type == load_type
            assert session.status == "pending" # check default
            assert isinstance(session.session_id, str)
            assert isinstance(session.created_at, datetime)
            assert isinstance(session.updated_at, datetime)


    def test_invalid_load_type(self):
        base_data = {
            'business_id': 'biz123', 'original_filename': 'file.csv',
            'wasabi_path': 'path/to/file.csv', 'load_type': 'invalid_type'
        }
        with pytest.raises(ValidationError) as excinfo:
            UploadSessionModel(**base_data)
        assert "load_type" in str(excinfo.value)
        assert "Invalid load_type" in str(excinfo.value)

    def test_default_values(self):
        session = UploadSessionModel(
            business_id='biz123',
            load_type='products',
            original_filename='file.csv',
            wasabi_path='path/to/file.csv'
        )
        assert session.status == "pending"
        assert session.details is None
        assert session.record_count is None
        assert session.error_count is None
        assert len(session.session_id) > 0 # UUID was generated
        assert (datetime.utcnow() - session.created_at).total_seconds() < 1 # approx now
        assert (datetime.utcnow() - session.updated_at).total_seconds() < 1 # approx now

    # Note: updated_at auto-update logic is not part of Pydantic model itself,
    # it's usually handled by ORM or application logic when saving.
    # So, we only test its default factory here.
