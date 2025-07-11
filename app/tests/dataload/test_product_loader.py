import pytest
from pydantic import ValidationError
from typing import Dict, Any

from app.dataload.models.product_csv import ProductCsvModel # generate_url_slug is internal to model
from app.dataload.product_loader import (
    load_product_record_to_db_refactored, # Target function for new tests
    parse_images, 
    parse_specifications,
    get_category_by_full_path_from_db # Might need to mock or use for setup
)
from app.db.models import (
    ProductOrm, BrandOrm, CategoryOrm, ReturnPolicyOrm, ShoppingCategoryOrm,
    ProductImageOrm, ProductSpecificationOrm, ProductsPriceHistoryOrm
)
from app.services.db_loaders import ErrorType # For DataLoaderError
from app.exceptions import DataLoaderError
from sqlalchemy.orm import Session
from unittest.mock import MagicMock, patch


# Assuming a test database session fixture `db_session` is available from a conftest.py
# For loader tests, we'll often mock the session and its query results.
# from app.db.models import BrandOrm, CategoryOrm, ReturnPolicyOrm, ShoppingCategoryOrm, ProductOrm, ProductImageOrm, ProductSpecificationOrm
# from sqlalchemy.orm import Session

# --- Unit Tests for Pydantic Model (ProductCsvModel) ---

def get_valid_product_csv_data(**overrides: Any) -> Dict[str, Any]:
    """ Provides valid raw data for ProductCsvModel, reflecting new structure """
    data = {
        "product_lookup_key": "LOOKUP-KEY-001", # New field
        "product_name": "Test Product Omega",
        "description": "A fantastic omega product for testing.",
        "brand_name": "OmegaBrand",
        "category_path": "Electronics / Gadgets / Testers", # For path-based lookup
        "shopping_category_name": "Test Gizmos",
        "price": 299.99,
        "sale_price": 279.99,
        "cost_price": 150.00,
        "quantity": 75,
        "package_size_length": 35.0,
        "package_size_width": 25.5,
        "package_size_height": 15.0,
        "product_weights": 3.0,
        "size_unit": "cm", # Use abbreviation for testing validator
        "weight_unit": "kg", # Use abbreviation
        "active": "ACTIVE",
        "return_type": "SALES_RETURN_ALLOWED",
        "return_fee_type": "FREE", # To test specific logic
        "return_fee": 0.0, # Pydantic model will handle if None and type is FREE
        "warehouse_location": "A1-R2-S3",
        "store_location": "Shelf 5, Aisle 3",
        "return_policy": "Standard 30 Day", # Name of policy for DB2 lookup
        "size_chart_img": "https://example.com/size_chart.png",
        "url": None, # Test auto-generation
        "video_url": "https://example.com/vid.mp4",
        "video_thumbnail_url": "https://example.com/vid_thumb.jpg",
        "images": "https://example.com/imgA.jpg|main_image:true|https://example.com/imgB.jpg|main_image:false",
        "specifications": "Feature:Awesome|Range:100m",
        "is_child_item": 0, # Test with 0, 1, and None
        "order_limit": 10, # New field
        "ean": "9876543210987",
        "isbn": "978-3-16-148410-0",
        "keywords": "omega, test, product",
        "mpn": "OMEGA-MPN-001",
        "seo_description": "Best omega testing product available.",
        "seo_title": "Omega Test Product - The Best",
        "upc": "192837465012",
    }
    data.update(overrides)
    return data

def test_product_csv_model_valid_data():
    data = get_valid_product_csv_data()
    model = ProductCsvModel(**data)
    assert model.product_name == "Test Product Omega"
    assert model.product_lookup_key == "LOOKUP-KEY-001"
    assert model.active == "ACTIVE"
    assert model.url == "test-product-omega" # Auto-generated
    assert model.return_fee_type == "FREE" # Stays FREE in model
    assert model.return_fee == 0.0 # Normalized by model if type is FREE
    assert model.size_unit == "CENTIMETERS" # Validated and transformed
    assert model.weight_unit == "KILOGRAMS" # Validated and transformed
    assert model.order_limit == 10
    assert model.is_child_item == 0

def test_product_csv_model_optional_fields_not_provided():
    data = get_valid_product_csv_data(
        sale_price=None,
        cost_price=None,
        shopping_category_name=None,
        return_fee_type=None, # Requires return_type SALES_ARE_FINAL or careful handling
        return_fee=None,
        warehouse_location=None,
        store_location=None,
        return_policy=None,
        size_chart_img="", # Test empty string becoming None
        video_url="",
        video_thumbnail_url=None, # Test None directly
        images=None,
        specifications="",
        is_child_item=None, # Test None
        order_limit=None,
        ean=None, isbn=None, keywords=None, mpn=None, seo_description=None, seo_title=None, upc=None,
        # For return_fee_type=None, need to ensure return_type allows it
        return_type="SALES_ARE_FINAL" 
    )
    model = ProductCsvModel(**data)
    assert model.sale_price is None
    assert model.size_chart_img is None
    assert model.video_url is None
    assert model.specifications is None
    assert model.is_child_item is None
    assert model.order_limit is None

def test_product_csv_model_url_provided_and_valid():
    data = get_valid_product_csv_data(url="my-custom-slug")
    model = ProductCsvModel(**data)
    assert model.url == "my-custom-slug"

def test_product_csv_model_url_provided_invalid():
    with pytest.raises(ValidationError, match="Provided URL is not a valid slug"):
        ProductCsvModel(**get_valid_product_csv_data(url="My Custom Slug!"))

# --- Tests for 'active' field in ProductCsvModel ---
def test_product_csv_model_active_missing_defaults_to_ACTIVE():
    data = get_valid_product_csv_data()
    del data['active'] # Simulate field missing from CSV row
    model = ProductCsvModel(**data)
    assert model.active == "ACTIVE"

def test_product_csv_model_active_empty_string_defaults_to_ACTIVE():
    model = ProductCsvModel(**get_valid_product_csv_data(active=""))
    assert model.active == "ACTIVE"

@pytest.mark.parametrize("active_input, expected_output", [
    ("ACTIVE", "ACTIVE"),
    ("active", "ACTIVE"),
    ("INACTIVE", "INACTIVE"),
    ("inactive", "INACTIVE"),
    (" Active ", "ACTIVE"), # Test stripping of whitespace
])
def test_product_csv_model_active_explicit_valid_values(active_input, expected_output):
    model = ProductCsvModel(**get_valid_product_csv_data(active=active_input))
    assert model.active == expected_output

def test_product_csv_model_active_invalid_string_value():
    with pytest.raises(ValidationError, match="Status, if provided and not empty, must be 'ACTIVE' or 'INACTIVE'"):
        ProductCsvModel(**get_valid_product_csv_data(active="Pending"))

def test_product_csv_model_active_invalid_type():
    with pytest.raises(ValidationError, match="Invalid type for active status"): # Match error from validator for non-str/None
        ProductCsvModel(**get_valid_product_csv_data(active=123))


def test_product_csv_model_return_type_invalid():
    with pytest.raises(ValidationError, match="return_type must be 'SALES_RETURN_ALLOWED' or 'SALES_ARE_FINAL'"):
        ProductCsvModel(**get_valid_product_csv_data(return_type="MAYBE_RETURN"))

# Return Policy Logic Tests (largely unchanged, just use new data getter)
def test_product_csv_model_return_sales_are_final_valid():
    data = get_valid_product_csv_data(return_type="SALES_ARE_FINAL", return_fee_type=None, return_fee=None)
    model = ProductCsvModel(**data) # This will fail if other fields are not compatible with SALES_ARE_FINAL
    assert model.return_type == "SALES_ARE_FINAL"
    assert model.return_fee_type is None
    assert model.return_fee is None

def test_product_csv_model_return_sales_are_final_with_fee_type():
    with pytest.raises(ValidationError, match="return_fee_type and return_fee must be null or empty when return_type is 'SALES_ARE_FINAL'"):
        ProductCsvModel(**get_valid_product_csv_data(return_type="SALES_ARE_FINAL", return_fee_type="FIXED", return_fee=None))

def test_product_csv_model_return_sales_are_final_with_fee():
    with pytest.raises(ValidationError, match="return_fee_type and return_fee must be null or empty when return_type is 'SALES_ARE_FINAL'"):
        ProductCsvModel(**get_valid_product_csv_data(return_type="SALES_ARE_FINAL", return_fee_type=None, return_fee=5.0))

def test_product_csv_model_return_allowed_no_fee_type():
    with pytest.raises(ValidationError, match="return_fee_type is required"):
        ProductCsvModel(**get_valid_product_csv_data(return_type="SALES_RETURN_ALLOWED", return_fee_type=None))

def test_product_csv_model_return_allowed_invalid_fee_type():
    with pytest.raises(ValidationError, match="return_fee_type, if provided, must be 'FIXED', 'PERCENTAGE', or 'FREE'"):
        ProductCsvModel(**get_valid_product_csv_data(return_type="SALES_RETURN_ALLOWED", return_fee_type="SOMETHING"))

def test_product_csv_model_return_allowed_free_fee_normalization():
    data = get_valid_product_csv_data(return_type="SALES_RETURN_ALLOWED", return_fee_type="FREE", return_fee=None)
    model = ProductCsvModel(**data)
    assert model.return_fee == 0.0

    data_with_fee_zero = get_valid_product_csv_data(return_type="SALES_RETURN_ALLOWED", return_fee_type="FREE", return_fee=0.0)
    model_zero = ProductCsvModel(**data_with_fee_zero)
    assert model_zero.return_fee == 0.0

def test_product_csv_model_return_allowed_free_fee_invalid():
    with pytest.raises(ValidationError, match="return_fee must be 0 or null/empty if return_fee_type is 'FREE'"):
        ProductCsvModel(**get_valid_product_csv_data(return_type="SALES_RETURN_ALLOWED", return_fee_type="FREE", return_fee=5.0))

def test_product_csv_model_return_allowed_fixed_no_fee():
    with pytest.raises(ValidationError, match="return_fee must be provided and non-negative if return_fee_type is 'FIXED'"):
        ProductCsvModel(**get_valid_product_csv_data(return_type="SALES_RETURN_ALLOWED", return_fee_type="FIXED", return_fee=None))

def test_product_csv_model_return_allowed_fixed_negative_fee():
    with pytest.raises(ValidationError, match="Price/fee fields, if provided, must be non-negative"): # This is a general validator for price fields
        ProductCsvModel(**get_valid_product_csv_data(return_type="SALES_RETURN_ALLOWED", return_fee_type="FIXED", return_fee=-5.0))

def test_product_csv_model_is_child_item_invalid_value():
    with pytest.raises(ValidationError, match="is_child_item must be 0 or 1 if provided"):
        ProductCsvModel(**get_valid_product_csv_data(is_child_item=2))

def test_product_csv_model_is_child_item_valid_optional():
    model = ProductCsvModel(**get_valid_product_csv_data(is_child_item=None))
    assert model.is_child_item is None
    model = ProductCsvModel(**get_valid_product_csv_data(is_child_item=1))
    assert model.is_child_item == 1


# --- New tests for unit and order_limit ---
@pytest.mark.parametrize("unit_input, expected_unit", [
    ("cm", "CENTIMETERS"), ("CM", "CENTIMETERS"), (" centimeters ", "CENTIMETERS"),
    ("m", "METERS"), ("M", "METERS"),
    ("ft", "FOOTS"), ("FT", "FOOTS"), ("foot", "FOOTS"),
    ("in", "INCHES"),
    ("mm", "MILLIMETERS"),
    ("METERS", "METERS"), # Already correct case
])
def test_product_csv_model_size_unit_validation(unit_input, expected_unit):
    model = ProductCsvModel(**get_valid_product_csv_data(size_unit=unit_input))
    assert model.size_unit == expected_unit

def test_product_csv_model_invalid_size_unit():
    with pytest.raises(ValidationError, match="Invalid size_unit: 'yards'"):
        ProductCsvModel(**get_valid_product_csv_data(size_unit="yards"))

@pytest.mark.parametrize("unit_input, expected_unit", [
    ("kg", "KILOGRAMS"), ("KG", "KILOGRAMS"), (" kilograms ", "KILOGRAMS"),
    ("g", "GRAMS"),
    ("lb", "POUNDS"),
    ("oz", "OUNCES"),
    ("mg", "MILLIGRAM"), # Ensure maps to MILLIGRAM (singular)
    ("t", "METRIC_TON"), ("tonne", "METRIC_TON"),
    ("ton", "TON"), # Test 'ton' maps to 'TON'
    ("KILOGRAMS", "KILOGRAMS"),
    ("METRIC_TON", "METRIC_TON"),
])
def test_product_csv_model_weight_unit_validation(unit_input, expected_unit):
    model = ProductCsvModel(**get_valid_product_csv_data(weight_unit=unit_input))
    assert model.weight_unit == expected_unit

def test_product_csv_model_invalid_weight_unit():
    with pytest.raises(ValidationError, match="Invalid weight_unit: 'stones'"):
        ProductCsvModel(**get_valid_product_csv_data(weight_unit="stones"))

def test_product_csv_model_order_limit():
    model = ProductCsvModel(**get_valid_product_csv_data(order_limit=50))
    assert model.order_limit == 50
    model_none = ProductCsvModel(**get_valid_product_csv_data(order_limit=None))
    assert model_none.order_limit is None
    # Pydantic automatically handles type conversion for int if it's a valid number string,
    # or raises error if it's not a valid int. Empty string for Optional[int] might need a pre-validator if not handled by default.
    model_empty_str = ProductCsvModel(**get_valid_product_csv_data(order_limit="")) 
    assert model_empty_str.order_limit is None # Validator should handle "" -> None
    
    with pytest.raises(ValidationError): # Test that invalid string for int fails
         ProductCsvModel(**get_valid_product_csv_data(order_limit="not-an-int"))


# Image and Specification String Parsing Tests (using the helper functions for direct unit testing)
# These tests are for parse_images and parse_specifications which are used by the model.
# They should largely remain the same, just ensuring they are called with get_valid_product_csv_data
def test_parse_images_valid_new_data(): # Renamed to avoid conflict if old tests are kept temporarily
    img_str = "http://a.com/1.jpg|main_image:true|http://b.com/2.png|main_image:false"
    # parse_images now returns dicts with "url" and "main_image"
    parsed = parse_images(img_str)
    assert len(parsed) == 2
    assert parsed[0] == {"url": "http://a.com/1.jpg", "main_image": True} # Corrected key to "url"
    assert parsed[1] == {"url": "http://b.com/2.png", "main_image": False} # Corrected key to "url"

def test_parse_images_invalid_url_new_data():
    img_str = "not_a_url|main_image:true"
    parsed = parse_images(img_str) 
    assert len(parsed) == 0
    with pytest.raises(ValidationError, match="Image URL 'not_a_url' must be a valid URL"):
        ProductCsvModel(**get_valid_product_csv_data(images=img_str))

def test_parse_images_invalid_flag_new_data():
    img_str = "http://a.com/1.jpg|main_image:maybe"
    parsed = parse_images(img_str)
    assert len(parsed) == 0
    with pytest.raises(ValidationError, match="Image flag 'main_image:maybe' must be 'main_image:true' or 'main_image:false'"):
        ProductCsvModel(**get_valid_product_csv_data(images=img_str))

def test_parse_images_odd_parts_new_data():
    img_str = "http://a.com/1.jpg|main_image:true|http://b.com/2.png"
    parsed = parse_images(img_str)
    assert len(parsed) == 0
    with pytest.raises(ValidationError, match="Images string must have pairs of url and main_image flag"):
        ProductCsvModel(**get_valid_product_csv_data(images=img_str))

def test_parse_specifications_valid_new_data():
    spec_str = "Color:Red| Size : Large |Material:Cotton Blend"
    parsed = parse_specifications(spec_str)
    assert len(parsed) == 3
    assert parsed[0] == {"name": "Color", "value": "Red"}
    assert parsed[1] == {"name": "Size", "value": "Large"}
    assert parsed[2] == {"name": "Material", "value": "Cotton Blend"}

def test_parse_specifications_malformed_pair_no_colon_new_data():
    spec_str = "ColorRed|Size:Large"
    parsed = parse_specifications(spec_str) 
    assert len(parsed) == 1
    assert parsed[0] == {"name": "Size", "value": "Large"}
    with pytest.raises(ValidationError, match="Specification entry 'ColorRed' must be in 'Name:Value' format"):
         ProductCsvModel(**get_valid_product_csv_data(specifications=spec_str))

def test_parse_specifications_empty_name_or_value_new_data():
    spec_str = ":Value|Name:|Name2:Value2" 
    parsed = parse_specifications(spec_str) 
    assert len(parsed) == 1
    assert parsed[0] == {"name": "Name2", "value": "Value2"}
    with pytest.raises(ValidationError, match="Specification entry ':Value' must be in 'Name:Value' format and both Name and Value must be non-empty."):
         ProductCsvModel(**get_valid_product_csv_data(specifications=":Value|Name2:Value2"))
    with pytest.raises(ValidationError, match="Specification entry 'Name:' must be in 'Name:Value' format and both Name and Value must be non-empty."):
         ProductCsvModel(**get_valid_product_csv_data(specifications="Name:|Name2:Value2"))


# --- Unit Tests for load_product_record_to_db_refactored ---
# (These will be new or heavily modified from old loader tests)

@pytest.fixture
def mock_db_session_for_loader():
    """Mocks an SQLAlchemy Session and its query capabilities."""
    session = MagicMock(spec=Session)
    
    # Mock query chain: session.query(Model).filter_by(...).one_or_none() / .all() / .first()
    mock_query_obj = MagicMock()
    session.query.return_value = mock_query_obj
    mock_query_obj.filter_by.return_value = mock_query_obj
    mock_query_obj.filter.return_value = mock_query_obj # For general filters
    
    # Specific return values will be set per test
    mock_query_obj.one_or_none.return_value = None 
    mock_query_obj.all.return_value = []
    mock_query_obj.first.return_value = None
    
    # Mock add, flush, delete, commit, rollback
    session.add = MagicMock()
    session.flush = MagicMock()
    session.delete = MagicMock()
    session.commit = MagicMock()
    session.rollback = MagicMock()
    return session

@pytest.fixture
def mock_product_csv_model(request) -> ProductCsvModel:
    """
    Creates a ProductCsvModel instance.
    If request.param is a dict, it's used as overrides for get_valid_product_csv_data.
    If request.param is the string "active_missing_scenario", the 'active' key is removed from data.
    """
    raw_data_overrides = {}
    delete_active_key = False

    if hasattr(request, "param"):
        if isinstance(request.param, dict):
            # If param is a dict, it could be raw_data itself or overrides
            # For this fixture, assume if it's a dict from parametrize, it's the full data for ProductCsvModel
            # This simplifies the parametrization for direct data dicts.
            # Let's refine: if request.param is a dict, it's specific overrides.
             raw_data_overrides = request.param
        elif request.param == "active_missing_scenario":
            delete_active_key = True
        # Can add more special string markers for other scenarios if needed
    
    # Ensure 'active' is not in overrides if we plan to delete it
    # This logic is a bit complex if overrides also contain 'active'
    # A clearer approach for parametrize: pass the *final data dict* or a marker.
    # Let's assume parametrize passes dicts that are *overrides* to the default valid data.
    
    current_overrides = {}
    if hasattr(request, "param"):
        if isinstance(request.param, dict):
            current_overrides = request.param
        elif request.param == "active_missing_scenario":
            delete_active_key = True # Will apply to default data
            # No specific overrides in this case, but could be combined if fixture was more complex
    
    data_for_model = get_valid_product_csv_data(**current_overrides)
    
    if delete_active_key:
        if 'active' in data_for_model: # Remove if it was part of default or current_overrides
            del data_for_model['active']
            
    return ProductCsvModel(**data_for_model)

@pytest.fixture
def mock_pre_resolved_category():
    """Provides a mock pre-resolved CategoryOrm object."""
    category = MagicMock(spec=CategoryOrm)
    category.id = 123
    category.name = "Mock Pre-resolved Category"
    # Add other attributes if load_product_record_to_db_refactored uses them (e.g. for leaf node check)
    return category

# Example of a test for the loader (will need many more)
@patch('app.dataload.product_loader.barcode_helper')
@pytest.mark.parametrize("csv_active_override, expected_db_active_val, expected_prod_type_status", [
    ({"active": "ACTIVE"}, "ACTIVE", 1),    # Explicitly ACTIVE from CSV
    ({"active": "active"}, "ACTIVE", 1),    # Case-insensitivity for ACTIVE
    ({"active": "INACTIVE"}, "INACTIVE", 2),# Explicitly INACTIVE from CSV
    ({"active": "inactive"}, "INACTIVE", 2),# Case-insensitivity for INACTIVE
    ({"active": ""}, "ACTIVE", 1),          # Empty string in CSV -> defaults to ACTIVE in DB
    ("active_missing_scenario", "ACTIVE", 1) # 'active' key missing from CSV data -> defaults to ACTIVE in DB
], indirect=["mock_product_csv_model"]) # mock_product_csv_model will receive the first element of each tuple
def test_load_product_refactored_new_product_creation_scenarios( # Renamed for clarity
    mock_barcode_helper, # Patched module
    mock_db_session_for_loader: Session, 
    mock_product_csv_model: ProductCsvModel, # Parametrized via csv_active_override
    expected_db_active_val: str,             # Second element of parametrize tuple
    expected_prod_type_status: int,          # Third element of parametrize tuple
    mock_pre_resolved_category: CategoryOrm  # Regular fixture
):
    # The mock_product_csv_model fixture is now responsible for creating the model
    # based on csv_active_override (which is request.param for the fixture)

    business_details_id = 1
    user_id = 100

    # --- Mocking DB Lookups ---
    # 1. Brand lookup: Assume brand exists
    mock_brand = BrandOrm(id=1, name=mock_product_csv_model.brand_name, business_details_id=business_details_id)
    # How queries are mocked depends on mock_db_session_for_loader setup
    # If filter_by returns the query object, then one_or_none is on that:
    mock_db_session_for_loader.query(BrandOrm).filter_by().one_or_none.return_value = mock_brand
    
    # 2. Category is pre-resolved (mock_pre_resolved_category)
    #    Leaf node check for category: Assume it's a leaf node
    mock_db_session_for_loader.query(CategoryOrm.id).filter().first.return_value = None 

    # 3. Shopping category lookup (optional): Assume it exists or is None
    if mock_product_csv_model.shopping_category_name:
        mock_sc = ShoppingCategoryOrm(id=1, name=mock_product_csv_model.shopping_category_name)
        mock_db_session_for_loader.query(ShoppingCategoryOrm).filter_by().one_or_none.return_value = mock_sc
    else:
        mock_db_session_for_loader.query(ShoppingCategoryOrm).filter_by().one_or_none.return_value = None
        
    # 4. Return policy lookup (DB2): Mock get_session for DB2 and the subsequent query
    mock_db2_session = MagicMock(spec=Session)
    mock_db2_query_obj = MagicMock()
    mock_db2_session.query.return_value = mock_db2_query_obj
    mock_db2_query_obj.filter.return_value = mock_db2_query_obj

    if mock_product_csv_model.return_policy:
        mock_rp = ReturnPolicyOrm(id=1, policy_name=mock_product_csv_model.return_policy, business_details_id=business_details_id)
        mock_db2_query_obj.one_or_none.return_value = mock_rp
    else:
        mock_db2_query_obj.one_or_none.return_value = None

    # Patch get_session to return our mock_db2_session when db_key="DB2"
    # This requires knowing how get_session is called or patching it globally if it's complex
    # For simplicity here, let's assume it's patchable directly in the loader's module:
    with patch('app.dataload.product_loader.get_session') as mock_get_db2_session:
        mock_get_db2_session.return_value = mock_db2_session

        # 5. Product lookup (for upsert): Assume product does not exist (is_new = True)
        # Lookup is by name and business_details_id
        mock_db_session_for_loader.query(ProductOrm).filter_by(
            name=mock_product_csv_model.product_name, # Changed to name
            business_details_id=business_details_id
        ).one_or_none.return_value = None

        # --- Mocking ID generation on flush ---
        # ProductOrm instance will be created. Mock 'add' to capture it.
        # Then, on 'flush', assign an ID to the captured instance.
        created_product_orm_instance_ref = {} # Use a dict to pass by reference effectively

        def capture_product_add(instance):
            if isinstance(instance, ProductOrm):
                created_product_orm_instance_ref['instance'] = instance
                # Assert placeholder values immediately after instantiation and before flush simulation
                assert instance.barcode == "TEMP_BARCODE_PENDING_ID"
                assert instance.mobile_barcode == "TEMP_MBARCODE_PENDING_ID"
                assert instance.self_gen_product_id.startswith("TEMP_SGPI_")
        mock_db_session_for_loader.add.side_effect = capture_product_add
        
        def assign_id_on_flush():
            if 'instance' in created_product_orm_instance_ref:
                created_product_orm_instance_ref['instance'].id = 999 # Assign a mock ID
        mock_db_session_for_loader.flush.side_effect = assign_id_on_flush
        
        # --- Mocking barcode_helper ---
        mock_barcode_helper.generate_barcode_image.return_value = b"fake_barcode_bytes"
        mock_barcode_helper.encode_barcode_to_base64.return_value = "FakeBase64String=="

        # --- Call the function under test ---
        product_id = load_product_record_to_db_refactored(
            db=mock_db_session_for_loader,
            business_details_id=business_details_id,
            product_data=mock_product_csv_model,
            session_id="test_session_123", # session_id is not used by this specific function for core logic
            user_id=user_id,
            pre_resolved_category=mock_pre_resolved_category
        )

    # --- Assertions ---
    assert product_id == 999
    mock_db_session_for_loader.add.assert_called() # Check add was called multiple times (product, specs, images, price history)
    
    # Assert ProductOrm instance fields (captured_product_orm_instance_ref['instance'])
    created_product_orm_instance = created_product_orm_instance_ref.get('instance')
    assert created_product_orm_instance is not None
    
    assert created_product_orm_instance.name == mock_product_csv_model.product_name
    # Final values after ID generation and update logic
    assert created_product_orm_instance.self_gen_product_id == "000000999" 
    assert created_product_orm_instance.mobile_barcode == "P999"
    assert created_product_orm_instance.barcode == "FakeBase64String=="
    
    # Assert active status based on parametrization
    assert created_product_orm_instance.active == expected_db_active_val
    assert created_product_orm_instance.product_type_status == expected_prod_type_status
    
    # This assertion should use the model's state AFTER pydantic processing, 
    # which includes defaulting for 'active'.
    # The loader logic is: if product_data.active (from model) == "INACTIVE" then status 2, else 1.
    # So, expected_db_active_val and expected_prod_type_status should align with this.
    # The parametrization is already set up to provide these expected DB values.

    if mock_product_csv_model.return_fee_type == "FREE": # This logic is independent of 'active' status
        assert created_product_orm_instance.return_fee_type == "FIXED"
        assert created_product_orm_instance.return_fee == 0.0
    else:
        assert created_product_orm_instance.return_fee_type == mock_product_csv_model.return_fee_type
        assert created_product_orm_instance.return_fee == mock_product_csv_model.return_fee
        
    assert created_product_orm_instance.size_unit == mock_product_csv_model.size_unit 
    assert created_product_orm_instance.weight_unit == mock_product_csv_model.weight_unit 
    assert created_product_orm_instance.is_child_item == mock_product_csv_model.is_child_item
    assert created_product_orm_instance.order_limit == mock_product_csv_model.order_limit

    mock_barcode_helper.generate_barcode_image.assert_called_once_with("P999", 350, 100)
    mock_barcode_helper.encode_barcode_to_base64.assert_called_once_with(b"fake_barcode_bytes")

    # Check if dependent data (images, specs, price history) was added
    # This requires checking the arguments to session.add for ProductImageOrm, ProductSpecificationOrm etc.
    # Example for image:
    if mock_product_csv_model.images:
        parsed_imgs_from_csv = parse_images(mock_product_csv_model.images)
        add_calls = mock_db_session_for_loader.add.call_args_list
        image_add_calls = [call for call in add_calls if isinstance(call[0][0], ProductImageOrm)]
        assert len(image_add_calls) == len(parsed_imgs_from_csv)
        # Further check attributes of added ProductImageOrm instances
        # Example: Check the first image added
        if parsed_imgs_from_csv:
            added_img_orm = image_add_calls[0][0][0] # The ProductImageOrm instance
            assert added_img_orm.product_id == 999
            assert added_img_orm.name == parsed_imgs_from_csv[0]['url']
            assert added_img_orm.main_image == parsed_imgs_from_csv[0]['main_image']
            assert created_product_orm_instance.main_image_url == parsed_imgs_from_csv[0]['url'] # Assuming first is main if true

    # Check if price history was added (since it's a new product)
    price_history_add_calls = [call for call in add_calls if isinstance(call[0][0], ProductsPriceHistoryOrm)]
    assert len(price_history_add_calls) == 1
    added_price_history_orm = price_history_add_calls[0][0][0]
    assert added_price_history_orm.product_id == 999
    assert added_price_history_orm.price == mock_product_csv_model.price
    assert added_price_history_orm.sale_price == mock_product_csv_model.sale_price
    assert added_price_history_orm.old_price is None # For new product


# --- Test for Product Update ---

def get_valid_product_csv_data_for_update_test(**overrides: Any) -> Dict[str, Any]:
    """ Provides valid raw data for ProductCsvModel for update scenarios. """
    data = {
        "product_name": "Test Product Omega", # This name will be used for lookup
        "description": "Updated fantastic omega product.",
        "brand_name": "OmegaBrand", 
        "category_path": "Electronics / Gadgets / Testers",
        "price": 309.99, # Updated price
        "quantity": 60, # Updated quantity
        "size_unit": "cm", 
        "weight_unit": "kg",
        "active": "ACTIVE",
        "return_type": "SALES_RETURN_ALLOWED",
        "return_fee_type": "FIXED", 
        "return_fee": 5.0,
        "package_size_length": 35.0,
        "package_size_width": 25.5,
        "package_size_height": 15.0,
        "product_weights": 3.0,
        "shopping_category_name": None,
        "warehouse_location": "WH-Updated", 
        "store_location": "Store-Updated", 
        "return_policy": "Standard 30 Day",
        "size_chart_img": None,
        "url": "test-product-omega-updated", 
        "video_url": None,
        "video_thumbnail_url": None,
        "images": "https://example.com/imgC.jpg|main_image:true", 
        "specifications": "Feature:Upgraded", 
        "is_child_item": 0, 
        "order_limit": 20, # Updated
        "ean": "9876543210988", # Updated EAN
        "isbn": None, 
        "keywords": "omega, updated", 
        "mpn": "OMEGA-MPN-001", # Usually stable for lookup if used
        "seo_description": "Updated SEO Desc.", 
        "seo_title": "Updated SEO Title", 
        "upc": "192837465013", # Updated UPC
    }
    data.update(overrides)
    return data

@patch('app.dataload.product_loader.barcode_helper') 
@patch('app.dataload.product_loader.get_session')    
def test_load_product_refactored_update_product_no_id_change(
    mock_get_db2_session, 
    mock_barcode_helper,
    mock_db_session_for_loader: Session, 
    mock_pre_resolved_category: CategoryOrm
):
    business_details_id = 1
    user_id = 101
    
    original_product_name = "Test Product Omega" 
    original_product_id = 555
    original_self_gen_id = str(original_product_id).zfill(9) 
    original_mobile_barcode = f"P{original_product_id}"       
    original_barcode_base64 = "OriginalBase64String=="
    initial_price = 290.0 # Different from update to trigger price history

    existing_product_orm = ProductOrm(
        id=original_product_id,
        name=original_product_name,
        business_details_id=business_details_id,
        self_gen_product_id=original_self_gen_id,
        mobile_barcode=original_mobile_barcode,
        barcode=original_barcode_base64,
        description="Initial description",
        price=initial_price, # Initial price
        sale_price = None,
        quantity=75,
        product_type_status=1, 
        active="ACTIVE",
        brand_name="OmegaBrand",
        category_id=mock_pre_resolved_category.id, 
        url="test-product-omega-initial",
        created_by=user_id-1, # Different user created
        created_date=1600000000, 
        updated_by=user_id-1,
        updated_date=1600000000,
         # Fill other NOT NULL fields from ProductOrm DDL
        package_size_length=35.0,
        package_size_width=25.5,
        package_size_height=15.0,
        product_weights=3.0,
        size_unit = "CENTIMETERS",
        weight_unit = "KILOGRAMS",
        return_type = "SALES_RETURN_ALLOWED", # Example
    )

    # Mock DB Lookups
    mock_brand = BrandOrm(id=1, name="OmegaBrand", business_details_id=business_details_id)
    mock_db_session_for_loader.query(BrandOrm).filter_by().one_or_none.return_value = mock_brand
    mock_db_session_for_loader.query(CategoryOrm.id).filter().first.return_value = None 
    mock_db_session_for_loader.query(ShoppingCategoryOrm).filter_by().one_or_none.return_value = None
        
    mock_db2_session = MagicMock(spec=Session)
    mock_db2_query_obj = MagicMock()
    mock_db2_session.query.return_value = mock_db2_query_obj
    mock_rp = ReturnPolicyOrm(id=1, policy_name="Standard 30 Day", business_details_id=business_details_id)
    mock_db2_query_obj.filter.return_value.one_or_none.return_value = mock_rp
    mock_get_db2_session.return_value = mock_db2_session

    mock_db_session_for_loader.query(ProductOrm).filter_by(
        name=original_product_name, 
        business_details_id=business_details_id
    ).one_or_none.return_value = existing_product_orm

    update_csv_data_dict = get_valid_product_csv_data_for_update_test(
        product_name=original_product_name, 
        description="Updated fantastic omega product.",
        price=309.99, # Price changed
        quantity=60,
        return_fee_type="FIXED", 
        return_fee=5.0,        
        images="https://example.com/imgC.jpg|main_image:true", 
        specifications="Feature:Upgraded", 
        order_limit=20 
    )
    product_csv_model_for_update = ProductCsvModel(**update_csv_data_dict)

    updated_product_id = load_product_record_to_db_refactored(
        db=mock_db_session_for_loader,
        business_details_id=business_details_id,
        product_data=product_csv_model_for_update,
        session_id="test_session_update_123",
        user_id=user_id, 
        pre_resolved_category=mock_pre_resolved_category
    )

    assert updated_product_id == original_product_id 

    # Key Assertions: Generated fields are NOT changed
    assert existing_product_orm.self_gen_product_id == original_self_gen_id
    assert existing_product_orm.mobile_barcode == original_mobile_barcode
    assert existing_product_orm.barcode == original_barcode_base64
    mock_barcode_helper.generate_barcode_image.assert_not_called()
    mock_barcode_helper.encode_barcode_to_base64.assert_not_called()

    # Assertions for updated fields
    assert existing_product_orm.description == "Updated fantastic omega product."
    assert existing_product_orm.price == 309.99
    assert existing_product_orm.quantity == 60
    assert existing_product_orm.order_limit == 20
    assert existing_product_orm.updated_by == user_id # Check updated_by is current user
    
    # Check price history was added due to price change
    add_calls = mock_db_session_for_loader.add.call_args_list
    added_price_history_orms = [call[0][0] for call in add_calls if isinstance(call[0][0], ProductsPriceHistoryOrm)]
    assert len(added_price_history_orms) == 1 
    if added_price_history_orms:
        history_entry = added_price_history_orms[0]
        assert history_entry.product_id == original_product_id
        assert history_entry.price == 309.99 
        assert history_entry.old_price == initial_price # Old actual price 
        assert history_entry.sale_price == product_csv_model_for_update.sale_price # new sale price (None in this case)
        
    mock_db_session_for_loader.flush.assert_called()
