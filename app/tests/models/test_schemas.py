import pytest
from pydantic import ValidationError
from datetime import datetime

from app.models.schemas import (
    BrandModel,
    AttributeModel,
    ReturnPolicyModel,
    ProductModel,
    ProductItemModel,
    ProductPriceModel,
    MetaTagModel,
    UploadSessionModel
)

# General utility for checking non-empty string validation
def check_non_empty_validator(model_class, field_name, valid_value="test", invalid_value=" "):
    # Test valid value
    model_instance = model_class(**{field_name: valid_value})
    assert getattr(model_instance, field_name) == valid_value

    # Test invalid value (empty or whitespace)
    with pytest.raises(ValidationError) as excinfo:
        model_class(**{field_name: invalid_value})
    assert field_name in str(excinfo.value).lower()
    assert "empty" in str(excinfo.value).lower()

# General utility for checking positive number validation
def check_positive_number_validator(model_class, field_name, valid_value=10, invalid_value=0):
    # Test valid value
    model_instance = model_class(**{field_name: valid_value})
    assert getattr(model_instance, field_name) == valid_value

    # Test invalid value (zero or negative)
    with pytest.raises(ValidationError) as excinfo:
        model_class(**{field_name: invalid_value})
    assert field_name in str(excinfo.value).lower()
    if invalid_value <=0: # specific message for positive
         assert "positive" in str(excinfo.value).lower()

# General utility for checking non-negative number validation
def check_non_negative_validator(model_class, field_name, valid_value=0, invalid_value=-1):
    model_instance = model_class(**{field_name: valid_value})
    assert getattr(model_instance, field_name) == valid_value
    with pytest.raises(ValidationError) as excinfo:
        model_class(**{field_name: invalid_value})
    assert field_name in str(excinfo.value).lower()
    assert "non-negative" in str(excinfo.value).lower()


class TestBrandModel:
    def test_brand_name_validation(self):
        check_non_empty_validator(BrandModel, "brand_name")

class TestAttributeModel:
    def test_attribute_name_validation(self):
        check_non_empty_validator(AttributeModel, "attribute_name", valid_value="Color")

    def test_allowed_values_validation(self):
        # For AttributeModel, allowed_values is also a string that shouldn't be empty
        check_non_empty_validator(AttributeModel, "allowed_values", valid_value="Red,Blue")

class TestReturnPolicyModel:
    def test_text_fields_validation(self):
        check_non_empty_validator(ReturnPolicyModel, "return_policy_code", valid_value="RP123")
        check_non_empty_validator(ReturnPolicyModel, "name", valid_value="Standard Policy")
        # description is not validated for non-empty, so it can be empty.
        ReturnPolicyModel(return_policy_code="c", name="n", return_window_days=1, grace_period_days=0, description="")


    def test_return_window_days_validation(self):
        check_positive_number_validator(ReturnPolicyModel, "return_window_days", valid_value=30, invalid_value=0)
        check_positive_number_validator(ReturnPolicyModel, "return_window_days", valid_value=30, invalid_value=-5)

    def test_grace_period_days_validation(self):
        check_non_negative_validator(ReturnPolicyModel, "grace_period_days", valid_value=0, invalid_value=-1)
        check_non_negative_validator(ReturnPolicyModel, "grace_period_days", valid_value=5, invalid_value=-1)


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
