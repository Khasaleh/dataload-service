import pytest
from pydantic import ValidationError
from typing import Dict, Any

from app.dataload.models.product_csv import ProductCsvModel, generate_url_slug
from app.dataload.product_loader import load_product_record_to_db, parse_images, parse_specifications

# Assuming a test database session fixture `db_session` is available from a conftest.py
# from app.db.models import BrandOrm, CategoryOrm, ReturnPolicyOrm, ShoppingCategoryOrm, ProductOrm, ProductImageOrm, ProductSpecificationOrm
# from sqlalchemy.orm import Session

# --- Unit Tests for Pydantic Model (ProductCsvModel) ---

def get_valid_product_data(**overrides: Any) -> Dict[str, Any]:
    data = {
        "product_name": "Test Product Alpha",
        "self_gen_product_id": "SKU-ALPHA-001",
        "business_details_id": 10, # Expected by model, though usually injected by loader task
        "description": "A great product for testing.",
        "brand_name": "AlphaBrand",
        "category_id": 101,
        "shopping_category_name": "ElectronicsTest",
        "price": 199.99,
        "sale_price": 179.99,
        "cost_price": 100.00,
        "quantity": 50,
        "package_size_length": 30.5,
        "package_size_width": 20.0,
        "package_size_height": 10.2,
        "product_weights": 2.5,
        "size_unit": "CM",
        "weight_unit": "KG",
        "active": "ACTIVE",
        "return_type": "SALES_RETURN_ALLOWED",
        "return_fee_type": "FIXED",
        "return_fee": 10.00,
        "url": None, # To test auto-generation
        "video_url": "https://example.com/video.mp4",
        "images": "https://example.com/img1.jpg|main_image:true|https://example.com/img2.jpg|main_image:false",
        "specifications": "Color:Red|Material:Metal",
        "is_child_item": 0,
        "ean": "1234567890123",
        "keywords": "test, product, alpha",
    }
    data.update(overrides)
    return data

def test_product_csv_model_valid_data():
    data = get_valid_product_data()
    model = ProductCsvModel(**data)
    assert model.product_name == "Test Product Alpha"
    assert model.active == "ACTIVE"
    assert model.url == "test-product-alpha" # Auto-generated
    assert model.return_fee == 10.00

def test_product_csv_model_url_provided_and_valid():
    data = get_valid_product_data(url="my-custom-slug")
    model = ProductCsvModel(**data)
    assert model.url == "my-custom-slug"

def test_product_csv_model_url_provided_invalid():
    with pytest.raises(ValidationError, match="Provided URL is not a valid slug"):
        ProductCsvModel(**get_valid_product_data(url="My Custom Slug!"))

def test_product_csv_model_active_status_invalid():
    with pytest.raises(ValidationError, match="Status must be 'ACTIVE' or 'INACTIVE'"):
        ProductCsvModel(**get_valid_product_data(active="Pending"))

def test_product_csv_model_return_type_invalid():
    with pytest.raises(ValidationError, match="return_type must be 'SALES_RETURN_ALLOWED' or 'SALES_ARE_FINAL'"):
        ProductCsvModel(**get_valid_product_data(return_type="MAYBE_RETURN"))

# Return Policy Logic Tests
def test_product_csv_model_return_sales_are_final_valid():
    data = get_valid_product_data(return_type="SALES_ARE_FINAL", return_fee_type=None, return_fee=None)
    model = ProductCsvModel(**data)
    assert model.return_type == "SALES_ARE_FINAL"
    assert model.return_fee_type is None
    assert model.return_fee is None

def test_product_csv_model_return_sales_are_final_with_fee_type():
    # The model validator will catch this as a broader condition violation
    with pytest.raises(ValidationError, match="return_fee_type and return_fee must be null or empty when return_type is 'SALES_ARE_FINAL'"):
        ProductCsvModel(**get_valid_product_data(return_type="SALES_ARE_FINAL", return_fee_type="FIXED", return_fee=None))

def test_product_csv_model_return_sales_are_final_with_fee():
    # This specific check is more robust in the root_validator
    with pytest.raises(ValidationError, match="return_fee_type and return_fee must be null or empty when return_type is 'SALES_ARE_FINAL'"):
        ProductCsvModel(**get_valid_product_data(return_type="SALES_ARE_FINAL", return_fee_type=None, return_fee=5.0))

def test_product_csv_model_return_allowed_no_fee_type():
    with pytest.raises(ValidationError, match="return_fee_type is required"):
        ProductCsvModel(**get_valid_product_data(return_type="SALES_RETURN_ALLOWED", return_fee_type=None))

def test_product_csv_model_return_allowed_invalid_fee_type():
    with pytest.raises(ValidationError, match="return_fee_type, if provided, must be 'FIXED', 'PERCENTAGE', or 'FREE'"): # Adjusted match
        ProductCsvModel(**get_valid_product_data(return_type="SALES_RETURN_ALLOWED", return_fee_type="SOMETHING"))

def test_product_csv_model_return_allowed_free_fee_normalization():
    data = get_valid_product_data(return_type="SALES_RETURN_ALLOWED", return_fee_type="FREE", return_fee=None)
    model = ProductCsvModel(**data)
    assert model.return_fee == 0.0

    data_with_fee_zero = get_valid_product_data(return_type="SALES_RETURN_ALLOWED", return_fee_type="FREE", return_fee=0.0)
    model_zero = ProductCsvModel(**data_with_fee_zero)
    assert model_zero.return_fee == 0.0

def test_product_csv_model_return_allowed_free_fee_invalid():
    with pytest.raises(ValidationError, match="return_fee must be 0 or null/empty if return_fee_type is 'FREE'"):
        ProductCsvModel(**get_valid_product_data(return_type="SALES_RETURN_ALLOWED", return_fee_type="FREE", return_fee=5.0))

def test_product_csv_model_return_allowed_fixed_no_fee():
    with pytest.raises(ValidationError, match="return_fee must be provided and non-negative if return_fee_type is 'FIXED'"):
        ProductCsvModel(**get_valid_product_data(return_type="SALES_RETURN_ALLOWED", return_fee_type="FIXED", return_fee=None))

def test_product_csv_model_return_allowed_fixed_negative_fee():
    with pytest.raises(ValidationError, match="Price/fee fields, if provided, must be non-negative"):
        ProductCsvModel(**get_valid_product_data(return_type="SALES_RETURN_ALLOWED", return_fee_type="FIXED", return_fee=-5.0))

def test_product_csv_model_is_child_item_invalid():
    with pytest.raises(ValidationError, match="is_child_item must be 0 or 1"):
        ProductCsvModel(**get_valid_product_data(is_child_item=2))

# Image and Specification String Parsing Tests (using the helper functions for direct unit testing)
def test_parse_images_valid():
    img_str = "http://a.com/1.jpg|main_image:true|http://b.com/2.png|main_image:false"
    parsed = parse_images(img_str)
    assert len(parsed) == 2
    assert parsed[0] == {"name": "http://a.com/1.jpg", "main_image": True}
    assert parsed[1] == {"name": "http://b.com/2.png", "main_image": False}

def test_parse_images_invalid_url():
    img_str = "not_a_url|main_image:true"
    # The Pydantic model validator for images string calls parse_images,
    # but parse_images itself logs warnings and continues. The Pydantic validator will raise error.
    # Here, we test parse_images directly.
    parsed = parse_images(img_str) # parse_images itself doesn't raise error, logs and skips
    assert len(parsed) == 0
    # Test through Pydantic model for actual validation error
    with pytest.raises(ValidationError, match="Image URL 'not_a_url' must be a valid URL"):
        ProductCsvModel(**get_valid_product_data(images=img_str))


def test_parse_images_invalid_flag():
    img_str = "http://a.com/1.jpg|main_image:maybe"
    parsed = parse_images(img_str) # parse_images itself logs warnings and skips
    assert len(parsed) == 0
    with pytest.raises(ValidationError, match="Image flag 'main_image:maybe' must be 'main_image:true' or 'main_image:false'"):
        ProductCsvModel(**get_valid_product_data(images=img_str))


def test_parse_images_odd_parts():
    img_str = "http://a.com/1.jpg|main_image:true|http://b.com/2.png" # Missing flag for last image
    parsed = parse_images(img_str) # parse_images itself logs warnings and returns empty
    assert len(parsed) == 0
    with pytest.raises(ValidationError, match="Images string must have pairs of url and main_image flag"):
        ProductCsvModel(**get_valid_product_data(images=img_str))


def test_parse_specifications_valid():
    spec_str = "Color:Red| Size : Large |Material:Cotton Blend"
    parsed = parse_specifications(spec_str)
    assert len(parsed) == 3
    assert parsed[0] == {"name": "Color", "value": "Red"}
    assert parsed[1] == {"name": "Size", "value": "Large"}
    assert parsed[2] == {"name": "Material", "value": "Cotton Blend"}

def test_parse_specifications_malformed_pair_no_colon():
    spec_str = "ColorRed|Size:Large"
    parsed = parse_specifications(spec_str) # parse_specifications logs warning and skips malformed
    assert len(parsed) == 1
    assert parsed[0] == {"name": "Size", "value": "Large"}
    # Test through Pydantic model
    with pytest.raises(ValidationError, match="Specification entry 'ColorRed' must be in 'Name:Value' format"):
         ProductCsvModel(**get_valid_product_data(specifications=spec_str))


def test_parse_specifications_empty_name_or_value():
    spec_str = ":Value|Name:|Name2:Value2" # First two are malformed
    parsed = parse_specifications(spec_str) # parse_specifications logs warning and skips
    assert len(parsed) == 1
    assert parsed[0] == {"name": "Name2", "value": "Value2"}
    # Test through Pydantic model
    with pytest.raises(ValidationError, match="Specification entry ':Value' must be in 'Name:Value' format and both Name and Value must be non-empty."):
         ProductCsvModel(**get_valid_product_data(specifications=":Value|Name2:Value2"))
    with pytest.raises(ValidationError, match="Specification entry 'Name:' must be in 'Name:Value' format and both Name and Value must be non-empty."):
         ProductCsvModel(**get_valid_product_data(specifications="Name:|Name2:Value2"))


# --- Unit Tests for load_product_record_to_db ---

@pytest.fixture
def mock_product_csv_model_data(request):
    # Allows overriding parts of get_valid_product_data for different test cases
    # Example: @pytest.mark.parametrize("mock_product_csv_model_data", [{"brand_name": "NonExistent"}], indirect=True)
    overrides = {}
    if hasattr(request, "param"):
        overrides = request.param

    valid_data = get_valid_product_data(**overrides)
    # Ensure business_details_id is present as ProductCsvModel expects it,
    # even if it's usually injected by the task in real flow.
    # The loader itself receives it as a separate arg, but model might be created with it.
    if 'business_details_id' not in valid_data: # Should be in get_valid_product_data
        valid_data['business_details_id'] = 10 # Default if not in get_valid_product_data

    return ProductCsvModel(**valid_data)


def test_load_product_brand_lookup_fails(mock_db_session, mock_product_csv_model_data):
    mock_db_session.query(BrandOrm).filter().one_or_none.return_value = None # Simulate brand not found

    with pytest.raises(DataLoaderError) as excinfo:
        load_product_record_to_db(mock_db_session, 10, mock_product_csv_model_data, "test_session_brand_fail")

    assert excinfo.value.error_type == ErrorType.LOOKUP
    assert excinfo.value.field_name == "brand_name"
    assert excinfo.value.offending_value == mock_product_csv_model_data.brand_name
    assert f"Brand '{mock_product_csv_model_data.brand_name}' not found" in excinfo.value.message

def test_load_product_category_lookup_fails(mock_db_session, mock_product_csv_model_data):
    # Setup successful brand lookup
    mock_db_session.query(BrandOrm).filter().one_or_none.return_value = MagicMock(spec=BrandOrm)
    # Simulate category not found
    mock_db_session.query(CategoryOrm).filter().one_or_none.return_value = None

    with pytest.raises(DataLoaderError) as excinfo:
        load_product_record_to_db(mock_db_session, 10, mock_product_csv_model_data, "test_session_cat_fail")

    assert excinfo.value.error_type == ErrorType.LOOKUP
    assert excinfo.value.field_name == "category_id"
    assert excinfo.value.offending_value == mock_product_csv_model_data.category_id
    assert f"Category with id '{mock_product_csv_model_data.category_id}' not found" in excinfo.value.message

def test_load_product_return_policy_lookup_fails(mock_db_session, mock_product_csv_model_data):
    mock_db_session.query(BrandOrm).filter().one_or_none.return_value = MagicMock(spec=BrandOrm)
    mock_db_session.query(CategoryOrm).filter().one_or_none.return_value = MagicMock(spec=CategoryOrm)
    if mock_product_csv_model_data.shopping_category_name: # If shopping category is part of test data
        mock_db_session.query(ShoppingCategoryOrm).filter().one_or_none.return_value = MagicMock(spec=ShoppingCategoryOrm)

    # Simulate return policy not found
    mock_db_session.query(ReturnPolicyOrm).filter().one_or_none.return_value = None

    with pytest.raises(DataLoaderError) as excinfo:
        load_product_record_to_db(mock_db_session, 10, mock_product_csv_model_data, "test_session_rp_fail")

    assert excinfo.value.error_type == ErrorType.LOOKUP
    assert excinfo.value.field_name == "return_type" # As per current loader logic
    assert "No matching ReturnPolicy found" in excinfo.value.message

def test_load_product_new_id_flush_fails(mock_db_session, mock_product_csv_model_data):
    mock_db_session.query(BrandOrm).filter().one_or_none.return_value = MagicMock(spec=BrandOrm)
    mock_db_session.query(CategoryOrm).filter().one_or_none.return_value = MagicMock(spec=CategoryOrm)
    if mock_product_csv_model_data.shopping_category_name:
        mock_db_session.query(ShoppingCategoryOrm).filter().one_or_none.return_value = MagicMock(spec=ShoppingCategoryOrm)
    mock_db_session.query(ReturnPolicyOrm).filter().one_or_none.return_value = MagicMock(spec=ReturnPolicyOrm)

    # Simulate creating a new product (product_orm_instance is None initially)
    mock_db_session.query(ProductOrm).filter().one_or_none.return_value = None

    # Simulate flush not setting an ID
    def mock_add_and_flush(instance):
        if isinstance(instance, ProductOrm):
            instance.id = None # Simulate ID not being set by flush
    mock_db_session.add.side_effect = mock_add_and_flush
    # No need to mock flush itself to raise error, just check if id is None after

    with pytest.raises(DataLoaderError) as excinfo:
        load_product_record_to_db(mock_db_session, 10, mock_product_csv_model_data, "test_session_flush_fail")

    assert excinfo.value.error_type == ErrorType.DATABASE
    assert excinfo.value.field_name == "self_gen_product_id"
    assert "DB flush failed to return an ID" in excinfo.value.message

def test_load_product_integrity_error_on_save(mock_db_session, mock_product_csv_model_data):
    mock_db_session.query(BrandOrm).filter().one_or_none.return_value = MagicMock(spec=BrandOrm)
    # ... other successful lookups ...
    mock_db_session.query(CategoryOrm).filter().one_or_none.return_value = MagicMock(spec=CategoryOrm)
    mock_db_session.query(ReturnPolicyOrm).filter().one_or_none.return_value = MagicMock(spec=ReturnPolicyOrm)

    mock_db_session.query(ProductOrm).filter().one_or_none.return_value = None # New product

    # Simulate IntegrityError on flush (could happen after add or during spec/image adds if complex)
    mock_db_session.flush.side_effect = IntegrityError("mock integrity error", params={}, orig=Exception("DB constraint violation"))

    with pytest.raises(DataLoaderError) as excinfo:
        load_product_record_to_db(mock_db_session, 10, mock_product_csv_model_data, "test_session_integrity_err")

    assert excinfo.value.error_type == ErrorType.DATABASE
    assert "Database integrity error" in excinfo.value.message
    assert "DB constraint violation" in excinfo.value.message # Check original error
    assert excinfo.value.field_name == "product_record_integrity_constraint"


# Placeholder for existing integration tests if any, or to be fully developed with DB interaction
# @pytest.fixture(scope="function")
# def setup_lookups(db_session: Session): # This would be a real DB session
#     # Create BrandOrm("AlphaBrand", business_details_id=10) -> brand_id_1
#     # Create CategoryOrm(id=101, name="Test Category", business_details_id=10)
#     # Create ShoppingCategoryOrm(name="ElectronicsTest") -> sc_id_1
#     # Create ReturnPolicyOrm for:
#     #   - type="SALES_RETURN_ALLOWED", fee_type="FIXED", fee=10.00, business_details_id=10 -> rp_id_1
#     #   - type="SALES_ARE_FINAL", fee_type=None, fee=None, business_details_id=10 -> rp_id_2
#     # db_session.commit()
#     # return {"brand_id_1": ..., "sc_id_1": ..., "rp_id_1": ..., "rp_id_2": ...}
#     pass

# def test_load_product_record_create_success(db_session: Session, setup_lookups):
#     valid_data_dict = get_valid_product_data(
#         brand_name="AlphaBrand", # Must match created BrandOrm
#         category_id=101, # Must match created CategoryOrm
#         shopping_category_name="ElectronicsTest", # Must match created ShoppingCategoryOrm
#         return_type="SALES_RETURN_ALLOWED",
#         return_fee_type="FIXED",
#         return_fee=10.00 # Must match a created ReturnPolicyOrm
#     )
#     product_model = ProductCsvModel(**valid_data_dict)

#     product_db_id = load_product_record_to_db(
#         db=db_session,
#         business_details_id=10,
#         product_data=product_model,
#         session_id="test_session_create"
#     )
#     assert product_db_id is not None
#     # Query ProductOrm, ProductImageOrm, ProductSpecificationOrm to verify data
#     # product = db_session.query(ProductOrm).get(product_db_id)
#     # assert product.name == "Test Product Alpha"
#     # assert product.return_policy_id == setup_lookups["rp_id_1"]
#     # assert len(product.images) == 2
#     # assert len(product.specifications) == 2
#     pass

# def test_load_product_record_update_success(db_session: Session, setup_lookups):
#     # 1. Create an initial product
#     initial_data = get_valid_product_data(self_gen_product_id="SKU-UPDATE-001", product_name="Initial Name")
#     initial_model = ProductCsvModel(**initial_data)
#     initial_id = load_product_record_to_db(db_session, 10, initial_model, "test_session_update_init")
#     assert initial_id is not None

#     # 2. Update the product
#     update_data_dict = get_valid_product_data(
#         self_gen_product_id="SKU-UPDATE-001", # Same key
#         product_name="Updated Product Name",
#         description="Updated description.",
#         images="https://example.com/new_img.jpg|main_image:true", # New images
#         specifications="NewSpec:NewValue" # New specs
#     )
#     update_model = ProductCsvModel(**update_data_dict)
#     updated_id = load_product_record_to_db(db_session, 10, update_model, "test_session_update")

#     assert updated_id == initial_id
#     # Query and verify updated fields, images, specs (old ones should be gone)
#     pass

# def test_load_product_record_missing_brand(db_session: Session, setup_lookups):
#     data_dict = get_valid_product_data(brand_name="NonExistentBrand")
#     product_model = ProductCsvModel(**data_dict)
#     product_db_id = load_product_record_to_db(db_session, 10, product_model, "test_session_missing_brand")
#     assert product_db_id is None # Should fail due to brand lookup

# def test_load_product_record_missing_category(db_session: Session, setup_lookups):
#     data_dict = get_valid_product_data(category_id=9999) # Non-existent category
#     product_model = ProductCsvModel(**data_dict)
#     product_db_id = load_product_record_to_db(db_session, 10, product_model, "test_session_missing_cat")
#     assert product_db_id is None

# def test_load_product_record_missing_return_policy(db_session: Session, setup_lookups):
#     data_dict = get_valid_product_data(return_type="SALES_RETURN_ALLOWED", return_fee_type="PERCENTAGE", return_fee=15.0) # Assume this combo doesn't exist
#     product_model = ProductCsvModel(**data_dict)
#     product_db_id = load_product_record_to_db(db_session, 10, product_model, "test_session_missing_rp")
#     assert product_db_id is None

# def test_load_product_record_is_child_item_no_images_specs_on_product(db_session: Session, setup_lookups):
#     data_dict = get_valid_product_data(
#         is_child_item=1,
#         images="https://example.com/child_img.jpg|main_image:true", # These should not be processed for product
#         specifications="ChildSpec:ChildValue" # These should still be processed
#     )
#     product_model = ProductCsvModel(**data_dict)
#     product_db_id = load_product_record_to_db(db_session, 10, product_model, "test_session_child_item")
#     assert product_db_id is not None
#     # product = db_session.query(ProductOrm).get(product_db_id)
#     # assert len(product.images) == 0 # Images not processed for product if is_child_item=1
#     # assert len(product.specifications) == 1 # Specifications are always processed
#     pass
