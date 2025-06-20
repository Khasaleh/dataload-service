import pytest
from unittest.mock import MagicMock, patch, call, ANY
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError # For flush error test

# Import functions to test and also Redis utils to assert their calls after being patched
from app.services.db_loaders import load_category_to_db, load_brand_to_db, DB_PK_MAP_SUFFIX, add_to_id_map, get_from_id_map
from app.db.models import CategoryOrm, BrandOrm

MODULE_PATH_FOR_REDIS_UTILS = "app.services.db_loaders" # Used by patcher

@pytest.fixture
def mock_db_session():
    return MagicMock(spec=Session)

@pytest.fixture
def mock_redis_pipeline():
    return MagicMock()

# Autouse fixture to patch Redis utils for all tests in this module
@pytest.fixture(autouse=True)
def auto_patch_redis_utils(mocker): # Renamed to avoid confusion with direct imports
    # These mocks will replace the actual functions in db_loaders for the duration of tests in this module
    mocker.patch(f'{MODULE_PATH_FOR_REDIS_UTILS}.add_to_id_map', autospec=True)
    mocker.patch(f'{MODULE_PATH_FOR_REDIS_UTILS}.get_from_id_map', autospec=True)


# --- Tests for load_category_to_db ---

# No longer need @patch for add_to_id_map/get_from_id_map here due to autouse fixture
def test_load_category_l1_new(mock_db_session, mock_redis_pipeline): # Mocks are auto-applied
    business_details_id = 100
    session_id = "sess_test_l1_new"
@patch(f'{MODULE_PATH_FOR_REDIS_UTILS}.add_to_id_map')
def test_load_category_l1_new(mock_add_to_id_map, mock_get_from_id_map, mock_db_session, mock_redis_pipeline):
    business_details_id = 100
    session_id = "sess_test_l1_new"
    record_data = {
        "category_path": "Electronics",
        "description": "All electronic items",
        "name": "Electronics Main" # Explicit name provided
    }

    mock_get_from_id_map.return_value = None # Not in Redis
    mock_db_session.query(CategoryOrm).filter_by().first.return_value = None # Not in DB

    # Simulate ID assignment on flush
    new_cat_instance = None
    def capture_add(instance):
        nonlocal new_cat_instance
        # Simulate that the instance passed to add IS the one we care about
        instance.id = 123 # Simulate DB assigning an ID after flush
        new_cat_instance = instance
    mock_db_session.add.side_effect = capture_add

    returned_db_pk = load_category_to_db(
        mock_db_session, business_details_id, record_data, session_id, mock_redis_pipeline
    )

    assert returned_db_pk == 123
    mock_get_from_id_map.assert_called_once_with(session_id, f"categories{DB_PK_MAP_SUFFIX}", "Electronics")
    mock_db_session.query(CategoryOrm).filter_by(
        business_details_id=business_details_id, name="Electronics Main", parent_id=None
    ).first.assert_called_once()

    mock_db_session.add.assert_called_once()
    assert new_cat_instance is not None
    assert new_cat_instance.name == "Electronics Main"
    assert new_cat_instance.description == "All electronic items"
    assert new_cat_instance.parent_id is None
    assert new_cat_instance.business_details_id == business_details_id

    mock_db_session.flush.assert_called_once()
    mock_add_to_id_map.assert_called_once_with(
        session_id, f"categories{DB_PK_MAP_SUFFIX}", "Electronics", 123, pipeline=mock_redis_pipeline
    )

@patch(f'{MODULE_PATH_FOR_REDIS_UTILS}.get_from_id_map')
@patch(f'{MODULE_PATH_FOR_REDIS_UTILS}.add_to_id_map')
def test_load_category_l1_update_existing_in_db(mock_add_to_id_map, mock_get_from_id_map, mock_db_session, mock_redis_pipeline):
    business_details_id = 100
    session_id = "sess_test_l1_update"
    record_data = {
        "category_path": "Electronics",
        "description": "Updated electronics description",
        "name": "Electronics Main" # Name matches existing
    }

    mock_existing_category = MagicMock(spec=CategoryOrm)
    mock_existing_category.id = 123
    mock_existing_category.name = "Electronics Main"
    mock_existing_category.description = "Old description"
    mock_existing_category.business_details_id = business_details_id
    mock_existing_category.parent_id = None

    mock_get_from_id_map.return_value = None # Not in Redis
    mock_db_session.query(CategoryOrm).filter_by().first.return_value = mock_existing_category # Found in DB

    returned_db_pk = load_category_to_db(
        mock_db_session, business_details_id, record_data, session_id, mock_redis_pipeline
    )

    assert returned_db_pk == 123
    assert mock_existing_category.description == "Updated electronics description" # Check update
    mock_db_session.add.assert_not_called() # Should not add new
    # mock_db_session.flush() # Not strictly needed for updates unless ORM needs it, but good to check if called

    mock_add_to_id_map.assert_called_once_with(
        session_id, f"categories{DB_PK_MAP_SUFFIX}", "Electronics", 123, pipeline=mock_redis_pipeline
    )

@patch(f'{MODULE_PATH_FOR_REDIS_UTILS}.get_from_id_map')
@patch(f'{MODULE_PATH_FOR_REDIS_UTILS}.add_to_id_map')
def test_load_category_l1_l2_l3_all_new(mock_add_to_id_map, mock_get_from_id_map, mock_db_session, mock_redis_pipeline):
    business_details_id = 100
    session_id = "sess_test_l1_l2_l3_new"
    record_data = { # This data is for "Laptops"
        "category_path": "Electronics/Computers/Laptops",
        "name": "Awesome Laptops", # Explicit name for the last segment
        "description": "Latest Laptops",
        "enabled": True
    }

    mock_get_from_id_map.return_value = None # All new to Redis
    mock_db_session.query(CategoryOrm).filter_by().first.return_value = None # All new to DB

    # Simulate ID assignment on flush for multiple adds
    added_instances_map = {} # path -> instance
    current_mock_id = 0
    def capture_add_sequential_id(instance):
        nonlocal current_mock_id
        current_mock_id += 1
        instance.id = current_mock_id
        # Store by a key that helps identify which instance it is, e.g., its name and parent_id
        # For simplicity in test, we assume order of add -> flush -> get id
        if instance.name == "Electronics": added_instances_map["Electronics"] = instance
        elif instance.name == "Computers": added_instances_map["Computers"] = instance
        elif instance.name == "Awesome Laptops": added_instances_map["Laptops"] = instance # name from record_data

    mock_db_session.add.side_effect = capture_add_sequential_id

    returned_db_pk = load_category_to_db(
        mock_db_session, business_details_id, record_data, session_id, mock_redis_pipeline
    )

    assert returned_db_pk == 3 # ID of Laptops
    assert mock_db_session.add.call_count == 3
    assert mock_db_session.flush.call_count == 3 # Called after each add

    # Check Electronics
    assert added_instances_map["Electronics"].name == "Electronics"
    assert added_instances_map["Electronics"].parent_id is None
    assert added_instances_map["Electronics"].description == "Category: Electronics" # Default desc

    # Check Computers
    assert added_instances_map["Computers"].name == "Computers"
    assert added_instances_map["Computers"].parent_id == added_instances_map["Electronics"].id # Should be 1
    assert added_instances_map["Computers"].description == "Category: Computers" # Default desc

    # Check Laptops (uses name from record_data)
    assert added_instances_map["Laptops"].name == "Awesome Laptops"
    assert added_instances_map["Laptops"].parent_id == added_instances_map["Computers"].id # Should be 2
    assert added_instances_map["Laptops"].description == "Latest Laptops" # From record_data
    assert added_instances_map["Laptops"].enabled is True

    expected_redis_calls = [
        call(session_id, f"categories{DB_PK_MAP_SUFFIX}", "Electronics", 1, pipeline=mock_redis_pipeline),
        call(session_id, f"categories{DB_PK_MAP_SUFFIX}", "Electronics/Computers", 2, pipeline=mock_redis_pipeline),
        call(session_id, f"categories{DB_PK_MAP_SUFFIX}", "Electronics/Computers/Laptops", 3, pipeline=mock_redis_pipeline),
    ]
    mock_add_to_id_map.assert_has_calls(expected_redis_calls, any_order=False) # Order matters here

@patch(f'{MODULE_PATH_FOR_REDIS_UTILS}.get_from_id_map')
@patch(f'{MODULE_PATH_FOR_REDIS_UTILS}.add_to_id_map')
def test_load_category_l2_parent_l1_exists_in_redis(mock_add_to_id_map, mock_get_from_id_map, mock_db_session, mock_redis_pipeline):
    business_details_id = 100
    session_id = "sess_test_l2_parent_in_redis"
    record_data = {"category_path": "Electronics/Mobile", "description": "Mobile Phones"}

    # L1 "Electronics" is in Redis, L2 "Electronics/Mobile" is not
    mock_get_from_id_map.side_effect = lambda s_id, map_type, path: "1" if path == "Electronics" else None

    # "Mobile" (child of "Electronics" which has ID 1) is not in DB
    mock_db_session.query(CategoryOrm).filter_by(name="Mobile", parent_id=1, business_details_id=business_details_id).first.return_value = None

    new_mobile_instance = None
    def capture_add_mobile(instance):
        nonlocal new_mobile_instance
        instance.id = 2 # Simulate DB assigning ID 2 to Mobile
        new_mobile_instance = instance
    mock_db_session.add.side_effect = capture_add_mobile

    returned_db_pk = load_category_to_db(mock_db_session, business_details_id, record_data, session_id, mock_redis_pipeline)

    assert returned_db_pk == 2
    # DB query for "Electronics" should be skipped due to Redis hit
    mock_db_session.query(CategoryOrm).filter_by(name="Electronics", parent_id=None).first.assert_not_called()
    # DB query for "Mobile" should happen
    mock_db_session.query(CategoryOrm).filter_by(name="Mobile", parent_id=1, business_details_id=business_details_id).first.assert_called_once()

    mock_db_session.add.assert_called_once() # Only Mobile is added
    assert new_mobile_instance is not None
    assert new_mobile_instance.name == "Mobile"
    assert new_mobile_instance.parent_id == 1
    assert new_mobile_instance.description == "Mobile Phones"

    mock_add_to_id_map.assert_called_once_with(
        session_id, f"categories{DB_PK_MAP_SUFFIX}", "Electronics/Mobile", 2, pipeline=mock_redis_pipeline
    )

@patch(f'{MODULE_PATH_FOR_REDIS_UTILS}.get_from_id_map')
@patch(f'{MODULE_PATH_FOR_REDIS_UTILS}.add_to_id_map')
def test_load_category_l1_update_existing_in_redis(mock_add_to_id_map, mock_get_from_id_map, mock_db_session, mock_redis_pipeline):
    business_details_id = 100
    session_id = "sess_test_l1_update_redis"
    record_data = {
        "category_path": "Electronics",
        "description": "Updated electronics description via Redis",
        "name": "Electronics Main"
    }

    mock_existing_category_orm = MagicMock(spec=CategoryOrm)
    mock_existing_category_orm.id = 123
    mock_existing_category_orm.name = "Electronics Main" # or whatever name is in DB
    mock_existing_category_orm.description = "Old DB description"
    mock_existing_category_orm.business_details_id = business_details_id
    mock_existing_category_orm.parent_id = None

    # Simulate found in Redis
    mock_get_from_id_map.return_value = "123"
    # Simulate DB query finding the record for update
    mock_db_session.query(CategoryOrm).filter_by(id=123, business_details_id=business_details_id).first.return_value = mock_existing_category_orm

    returned_db_pk = load_category_to_db(
        mock_db_session, business_details_id, record_data, session_id, mock_redis_pipeline
    )

    assert returned_db_pk == 123
    mock_get_from_id_map.assert_called_once_with(session_id, f"categories{DB_PK_MAP_SUFFIX}", "Electronics")
    # Ensure the DB was queried for the update, as it's the last level.
    mock_db_session.query(CategoryOrm).filter_by(id=123, business_details_id=business_details_id).first.assert_called_once()
    assert mock_existing_category_orm.description == "Updated electronics description via Redis"
    mock_db_session.add.assert_not_called()
    # add_to_id_map should NOT be called again if it was found in Redis (current logic in load_category_to_db adds it regardless)
    # Let's adjust the test to reflect current behavior, or note this as a potential optimization in load_category_to_db
    # Current load_category_to_db will call add_to_id_map if category_db_id_from_redis_str is None.
    # If it IS found in redis, it does not call add_to_id_map again. This is correct.
    mock_add_to_id_map.assert_not_called() # Because it was found in Redis already


@patch(f'{MODULE_PATH_FOR_REDIS_UTILS}.get_from_id_map')
@patch(f'{MODULE_PATH_FOR_REDIS_UTILS}.add_to_id_map')
def test_load_category_l2_parent_l1_exists_in_db_not_redis(mock_add_to_id_map, mock_get_from_id_map, mock_db_session, mock_redis_pipeline):
    business_details_id = 100
    session_id = "sess_test_l2_parent_in_db"
    record_data = {"category_path": "Electronics/Wearables", "description": "Smart Watches and Bands"}

    # Simulate "Electronics" not in Redis, but "Electronics/Wearables" also not in Redis
    mock_get_from_id_map.return_value = None

    # DB Mocks:
    # 1. For "Electronics" (parent_id=None)
    mock_l1_electronics = MagicMock(spec=CategoryOrm)
    mock_l1_electronics.id = 1
    mock_l1_electronics.name = "Electronics"
    # 2. For "Wearables" (parent_id=1) - this one is new
    mock_db_session.query(CategoryOrm).filter_by().side_effect = [
        mock_l1_electronics, # First call for "Electronics"
        None                 # Second call for "Wearables"
    ]

    new_wearables_instance = None
    def capture_add_wearables(instance):
        nonlocal new_wearables_instance
        instance.id = 2 # Simulate DB assigning ID 2 to Wearables
        new_wearables_instance = instance
    mock_db_session.add.side_effect = capture_add_wearables

    returned_db_pk = load_category_to_db(mock_db_session, business_details_id, record_data, session_id, mock_redis_pipeline)

    assert returned_db_pk == 2 # ID of Wearables

    # Check Redis calls
    expected_get_redis_calls = [
        call(session_id, f"categories{DB_PK_MAP_SUFFIX}", "Electronics"),
        call(session_id, f"categories{DB_PK_MAP_SUFFIX}", "Electronics/Wearables"),
    ]
    mock_get_from_id_map.assert_has_calls(expected_get_redis_calls)

    # Check DB query calls
    expected_db_query_calls = [
        call(business_details_id=business_details_id, name="Electronics", parent_id=None), # For L1
        call(business_details_id=business_details_id, name="Wearables", parent_id=1)      # For L2
    ]
    mock_db_session.query(CategoryOrm).filter_by.assert_has_calls(expected_db_query_calls)

    # Check add_to_id_map calls (should be called for both as they were not in Redis initially)
    expected_add_redis_calls = [
        call(session_id, f"categories{DB_PK_MAP_SUFFIX}", "Electronics", 1, pipeline=mock_redis_pipeline),
        call(session_id, f"categories{DB_PK_MAP_SUFFIX}", "Electronics/Wearables", 2, pipeline=mock_redis_pipeline),
    ]
    mock_add_to_id_map.assert_has_calls(expected_add_redis_calls, any_order=False)

    mock_db_session.add.assert_called_once() # Only Wearables is added
    assert new_wearables_instance is not None
    assert new_wearables_instance.name == "Wearables"
    assert new_wearables_instance.parent_id == 1


@patch(f'{MODULE_PATH_FOR_REDIS_UTILS}.get_from_id_map', return_value=None) # All new
@patch(f'{MODULE_PATH_FOR_REDIS_UTILS}.add_to_id_map')
def test_load_category_path_name_in_record_data_used_for_last_level(mock_add_to_id_map, mock_get_from_id_map, mock_db_session, mock_redis_pipeline):
    business_details_id = 100
    session_id = "sess_test_name_override"
    record_data = {
        "category_path": "Audio/Speakers",
        "name": "High-End Speakers", # Explicit name for the last segment
        "description": "Premium Sound Systems"
    }

    mock_db_session.query(CategoryOrm).filter_by().first.return_value = None # All new

    added_instances = {}
    current_mock_id_name_test = 0
    def capture_add_name_test(instance):
        nonlocal current_mock_id_name_test
        current_mock_id_name_test += 1
        instance.id = current_mock_id_name_test
        added_instances[instance.name] = instance # Store by name
    mock_db_session.add.side_effect = capture_add_name_test

    returned_db_pk = load_category_to_db(mock_db_session, business_details_id, record_data, session_id, mock_redis_pipeline)

    assert returned_db_pk == 2 # ID for "High-End Speakers"
    assert mock_db_session.add.call_count == 2

    assert "Audio" in added_instances
    assert added_instances["Audio"].name == "Audio"
    assert added_instances["Audio"].parent_id is None

    assert "High-End Speakers" in added_instances # Check that name from record_data was used
    assert added_instances["High-End Speakers"].name == "High-End Speakers"
    assert added_instances["High-End Speakers"].parent_id == added_instances["Audio"].id
    assert added_instances["High-End Speakers"].description == "Premium Sound Systems"


def test_load_category_missing_category_path(mock_db_session, mock_redis_pipeline):
    record_data = {"name": "Category without path", "description": "Test"}
    returned_db_pk = load_category_to_db(
        mock_db_session, 100, record_data, "sess_missing_path", mock_redis_pipeline
    )
    assert returned_db_pk is None
    mock_db_session.add.assert_not_called()

@patch(f'{MODULE_PATH_FOR_REDIS_UTILS}.get_from_id_map', return_value=None) # Not in Redis
@patch(f'{MODULE_PATH_FOR_REDIS_UTILS}.add_to_id_map')
def test_load_category_db_flush_error(mock_add_to_id_map, mock_get_from_id_map, mock_db_session, mock_redis_pipeline):
    record_data = {"category_path": "Electronics", "description": "Test"}
    mock_db_session.query(CategoryOrm).filter_by().first.return_value = None # Not in DB
    mock_db_session.flush.side_effect = Exception("DB Flush Error") # Simulate error

    returned_db_pk = load_category_to_db(
        mock_db_session, 100, record_data, "sess_flush_error", mock_redis_pipeline
    )
    assert returned_db_pk is None
    mock_db_session.add.assert_called_once() # Add was attempted
    mock_add_to_id_map.assert_not_called() # Should not reach here if flush fails and returns None


# --- Tests for load_brand_to_db ---

def test_load_brand_new_record_success(mock_db_session, mock_redis_pipeline):
    # Access patched versions from autouse fixture if needed, or pass them if specific mock needed
    mock_add_to_id_map = getattr(patch_redis_utils, 'add_to_id_map_mock', MagicMock()) # Get from fixture or default

    business_details_id = 200
    session_id = "sess_brand_new"
    record_data = {
        "name": "Awesome Brand",
        "logo": "logo.png",
        "supplier_id": 55,
        "active": "TRUE",
        "created_by": 10, "created_date": 1678886400, # Example epoch
        "updated_by": 11, "updated_date": 1678886500,
    }

    mock_db_session.query(BrandOrm).filter_by().first.return_value = None # New record

    new_brand_instance = None
    def capture_add_brand(instance):
        nonlocal new_brand_instance
        instance.id = 789 # Simulate DB assigning an ID
        new_brand_instance = instance
    mock_db_session.add.side_effect = capture_add_brand

    returned_db_pk = load_brand_to_db(
        mock_db_session, business_details_id, record_data, session_id, mock_redis_pipeline
    )

    assert returned_db_pk == 789
    mock_db_session.query(BrandOrm).filter_by(
        business_details_id=business_details_id, name="Awesome Brand"
    ).first.assert_called_once()

    mock_db_session.add.assert_called_once()
    assert new_brand_instance is not None
    assert new_brand_instance.name == "Awesome Brand"
    assert new_brand_instance.logo == "logo.png"
    assert new_brand_instance.supplier_id == 55
    assert new_brand_instance.active == "TRUE"
    assert new_brand_instance.created_by == 10
    assert new_brand_instance.created_date == 1678886400
    assert new_brand_instance.business_details_id == business_details_id

    mock_db_session.flush.assert_called_once()
    # Use the imported (and now patched) add_to_id_map directly
    add_to_id_map.assert_called_once_with(
        session_id, f"brands{DB_PK_MAP_SUFFIX}", "Awesome Brand", 789, pipeline=mock_redis_pipeline
    )


def test_load_brand_update_existing_record_success(mock_db_session, mock_redis_pipeline):
    # add_to_id_map is auto-patched by auto_patch_redis_utils
    business_details_id = 200
    session_id = "sess_brand_update"
    record_data = {
        "name": "Awesome Brand", # Key to find existing
        "logo": "new_logo.png",   # Updated field
        "active": "FALSE",        # Updated field
        "supplier_id": None,      # Clearing a field
        "updated_by": 12,         # New audit info
        "updated_date": 1678887000
    }

    mock_existing_brand = BrandOrm(
        id=789, name="Awesome Brand", business_details_id=business_details_id,
        logo="old_logo.png", active="TRUE", supplier_id=55,
        created_by=10, created_date=1678886400, updated_by=10, updated_date=1678886400
    )
    mock_db_session.query(BrandOrm).filter_by().first.return_value = mock_existing_brand

    returned_db_pk = load_brand_to_db(
        mock_db_session, business_details_id, record_data, session_id, mock_redis_pipeline
    )

    assert returned_db_pk == 789
    mock_db_session.add.assert_not_called()
    assert mock_existing_brand.logo == "new_logo.png"
    assert mock_existing_brand.active == "FALSE"
    assert mock_existing_brand.supplier_id is None # Check if set to None
    assert mock_existing_brand.updated_by == 12
    assert mock_existing_brand.updated_date == 1678887000
    # Ensure non-provided fields are not changed from original
    assert mock_existing_brand.created_by == 10

    add_to_id_map.assert_called_once_with(
        session_id, f"brands{DB_PK_MAP_SUFFIX}", "Awesome Brand", 789, pipeline=mock_redis_pipeline
    )


def test_load_brand_optional_fields_not_provided_on_create(mock_db_session, mock_redis_pipeline):
    # add_to_id_map is auto-patched
    business_details_id = 200
    session_id = "sess_brand_optional"
    record_data = { # Only mandatory fields
        "name": "Minimal Brand",
        "logo": "minimal_logo.png",
    }
    mock_db_session.query(BrandOrm).filter_by().first.return_value = None

    new_brand_instance = None
    def capture_add_minimal_brand(instance):
        nonlocal new_brand_instance
        instance.id = 790
        new_brand_instance = instance
    mock_db_session.add.side_effect = capture_add_minimal_brand

    returned_db_pk = load_brand_to_db(mock_db_session, business_details_id, record_data, session_id, mock_redis_pipeline)

    assert returned_db_pk == 790
    assert new_brand_instance is not None
    assert new_brand_instance.name == "Minimal Brand"
    assert new_brand_instance.logo == "minimal_logo.png"
    assert new_brand_instance.supplier_id is None
    assert new_brand_instance.active is None
    assert new_brand_instance.created_by is None
    assert new_brand_instance.created_date is None


def test_load_brand_update_preserves_unprovided_fields(mock_db_session, mock_redis_pipeline):
    mock_add_to_id_map = getattr(patch_redis_utils, 'add_to_id_map_mock', MagicMock())
    business_details_id = 200
    session_id = "sess_brand_preserve"
    record_data = { # Only logo is updated
        "name": "Preserved Brand",
        "logo": "very_new_logo.png",
    }

    mock_existing_brand = BrandOrm(
        id=791, name="Preserved Brand", business_details_id=business_details_id,
        logo="old_logo_preserved.png", active="ACTIVE_STATUS", supplier_id=101,
        created_by=1, created_date=1000, updated_by=1, updated_date=1000
    )
    mock_db_session.query(BrandOrm).filter_by().first.return_value = mock_existing_brand

    returned_db_pk = load_brand_to_db(mock_db_session, business_details_id, record_data, session_id, mock_redis_pipeline)

    assert returned_db_pk == 791
    assert mock_existing_brand.logo == "very_new_logo.png" # Updated
    assert mock_existing_brand.active == "ACTIVE_STATUS" # Preserved
    assert mock_existing_brand.supplier_id == 101 # Preserved
    assert mock_existing_brand.updated_by is None
    assert mock_existing_brand.updated_date is None # These are not updated because they were not in record_data


def test_load_brand_missing_name_returns_none(mock_db_session, mock_redis_pipeline):
    record_data = {"logo": "some_logo.png"} # Name is missing
    returned_db_pk = load_brand_to_db(mock_db_session, 200, record_data, "sess_brand_no_name", mock_redis_pipeline)
    assert returned_db_pk is None
    mock_db_session.add.assert_not_called()
    add_to_id_map.assert_not_called() # add_to_id_map is auto-patched


def test_load_brand_db_flush_error_returns_none(mock_db_session, mock_redis_pipeline):
    # add_to_id_map is auto-patched
    record_data = {"name": "FlushFail Brand", "logo": "logo.png"}
    mock_db_session.query(BrandOrm).filter_by().first.return_value = None
    mock_db_session.flush.side_effect = SQLAlchemyError("DB Flush Error Simulated")

    returned_db_pk = load_brand_to_db(mock_db_session, 200, record_data, "sess_brand_flush_fail", mock_redis_pipeline)

    assert returned_db_pk is None
    mock_db_session.add.assert_called_once() # Add was attempted
    add_to_id_map.assert_not_called() # Should not be called if flush fails
```
