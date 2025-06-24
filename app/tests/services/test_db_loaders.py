import pytest
from unittest.mock import MagicMock, patch, call, ANY
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError, IntegrityError # For flush and bulk errors

# Import functions to test
from app.services.db_loaders import (
    load_category_to_db, load_brand_to_db, load_return_policy_to_db,
    DB_PK_MAP_SUFFIX
)
import app.services.db_loaders as db_loaders_module
from app.db.models import CategoryOrm, BrandOrm, ReturnPolicyOrm
from app.exceptions import DataLoaderError # Import custom exception
from app.models.schemas import ErrorType # Import ErrorType Enum
from datetime import datetime

MODULE_PATH_FOR_REDIS_UTILS = "app.services.db_loaders" # Path for mocking redis utils if they are in db_loaders
# If redis utils are in app.utils.redis_utils and imported by db_loaders, then this path might need adjustment
# For now, assuming db_loaders_module.add_to_id_map and .get_from_id_map correctly refers to them.

@pytest.fixture
def mock_db_session_for_loaders():
    return MagicMock(spec=Session)

@pytest.fixture
def mock_redis_pipeline_for_loaders():
    return MagicMock()

@pytest.fixture(autouse=True)
def auto_patch_redis_utils(mocker):
    mocker.patch(f'{MODULE_PATH_FOR_REDIS_UTILS}.add_to_id_map', autospec=True)
    mocker.patch(f'{MODULE_PATH_FOR_REDIS_UTILS}.get_from_id_map', autospec=True)


# --- Tests for load_category_to_db ---

def test_load_category_l1_new(mock_db_session_for_loaders, mock_redis_pipeline_for_loaders):
    business_details_id = 100
    session_id = "sess_test_l1_new"
    record_data = {
        "category_path": "Electronics",
        "description": "All electronic items",
        "name": "Electronics Main"
    }

    db_loaders_module.get_from_id_map.return_value = None
    mock_db_session_for_loaders.query(CategoryOrm).filter_by().first.return_value = None

    new_cat_instance = None
    def capture_add(instance):
        nonlocal new_cat_instance
        instance.id = 123
        new_cat_instance = instance
    mock_db_session_for_loaders.add.side_effect = capture_add

    returned_db_pk = load_category_to_db(
        mock_db_session_for_loaders, business_details_id, record_data, session_id, mock_redis_pipeline_for_loaders
    )

    assert returned_db_pk == 123
    db_loaders_module.get_from_id_map.assert_called_once_with(session_id, f"categories{DB_PK_MAP_SUFFIX}", "Electronics")
    mock_db_session_for_loaders.query(CategoryOrm).filter_by(
        business_details_id=business_details_id, name="Electronics Main", parent_id=None
    ).first.assert_called_once()

    mock_db_session_for_loaders.add.assert_called_once()
    assert new_cat_instance is not None
    assert new_cat_instance.name == "Electronics Main"
    assert new_cat_instance.description == "All electronic items"
    assert new_cat_instance.parent_id is None
    assert new_cat_instance.business_details_id == business_details_id

    mock_db_session_for_loaders.flush.assert_called_once()
    db_loaders_module.add_to_id_map.assert_called_once_with(
        session_id, f"categories{DB_PK_MAP_SUFFIX}", "Electronics", 123, pipeline=mock_redis_pipeline_for_loaders
    )

def test_load_category_l1_update_existing_in_db(mock_db_session_for_loaders, mock_redis_pipeline_for_loaders):
    business_details_id = 100
    session_id = "sess_test_l1_update"
    record_data = {
        "category_path": "Electronics",
        "description": "Updated electronics description",
        "name": "Electronics Main"
    }

    mock_existing_category = MagicMock(spec=CategoryOrm)
    mock_existing_category.id = 123
    mock_existing_category.name = "Electronics Main"
    mock_existing_category.description = "Old description"

    db_loaders_module.get_from_id_map.return_value = None
    mock_db_session_for_loaders.query(CategoryOrm).filter_by().first.return_value = mock_existing_category

    returned_db_pk = load_category_to_db(
        mock_db_session_for_loaders, business_details_id, record_data, session_id, mock_redis_pipeline_for_loaders
    )

    assert returned_db_pk == 123
    assert mock_existing_category.description == "Updated electronics description"
    mock_db_session_for_loaders.add.assert_not_called()
    db_loaders_module.add_to_id_map.assert_called_once_with(
        session_id, f"categories{DB_PK_MAP_SUFFIX}", "Electronics", 123, pipeline=mock_redis_pipeline_for_loaders
    )

def test_load_category_l1_l2_l3_all_new(mock_db_session_for_loaders, mock_redis_pipeline_for_loaders):
    business_details_id = 100
    session_id = "sess_test_l1_l2_l3_new"
    record_data = {
        "category_path": "Electronics/Computers/Laptops", "name": "Awesome Laptops", "description": "Latest Laptops", "enabled": True
    }
    db_loaders_module.get_from_id_map.return_value = None
    mock_db_session_for_loaders.query(CategoryOrm).filter_by().first.return_value = None

    added_instances_map = {}
    current_mock_id = 0
    def capture_add_sequential_id(instance):
        nonlocal current_mock_id
        current_mock_id += 1
        instance.id = current_mock_id
        if instance.name == "Electronics": added_instances_map["Electronics"] = instance
        elif instance.name == "Computers": added_instances_map["Computers"] = instance
        elif instance.name == "Awesome Laptops": added_instances_map["Laptops"] = instance
    mock_db_session_for_loaders.add.side_effect = capture_add_sequential_id

    returned_db_pk = load_category_to_db(
        mock_db_session_for_loaders, business_details_id, record_data, session_id, mock_redis_pipeline_for_loaders
    )
    assert returned_db_pk == 3
    expected_redis_calls = [
        call(session_id, f"categories{DB_PK_MAP_SUFFIX}", "Electronics", 1, pipeline=mock_redis_pipeline_for_loaders),
        call(session_id, f"categories{DB_PK_MAP_SUFFIX}", "Electronics/Computers", 2, pipeline=mock_redis_pipeline_for_loaders),
        call(session_id, f"categories{DB_PK_MAP_SUFFIX}", "Electronics/Computers/Laptops", 3, pipeline=mock_redis_pipeline_for_loaders),
    ]
    db_loaders_module.add_to_id_map.assert_has_calls(expected_redis_calls, any_order=False)


def test_load_category_l2_parent_l1_exists_in_redis(mock_db_session_for_loaders, mock_redis_pipeline_for_loaders):
    business_details_id = 100
    session_id = "sess_test_l2_parent_in_redis"
    record_data = {"category_path": "Electronics/Mobile", "description": "Mobile Phones"}
    db_loaders_module.get_from_id_map.side_effect = lambda s_id, map_type, path: "1" if path == "Electronics" else None
    mock_db_session_for_loaders.query(CategoryOrm).filter_by(name="Mobile", parent_id=1).first.return_value = None
    new_mobile_instance = None
    def capture_add_mobile(instance):
        nonlocal new_mobile_instance
        instance.id = 2
        new_mobile_instance = instance
    mock_db_session_for_loaders.add.side_effect = capture_add_mobile
    returned_db_pk = load_category_to_db(mock_db_session_for_loaders, business_details_id, record_data, session_id, mock_redis_pipeline_for_loaders)
    assert returned_db_pk == 2
    db_loaders_module.add_to_id_map.assert_called_once_with(
        session_id, f"categories{DB_PK_MAP_SUFFIX}", "Electronics/Mobile", 2, pipeline=mock_redis_pipeline_for_loaders
    )

def test_load_category_l1_update_existing_in_redis(mock_db_session_for_loaders, mock_redis_pipeline_for_loaders):
    business_details_id = 100; session_id = "sess_test_l1_update_redis"
    record_data = {"category_path": "Electronics", "description": "Updated via Redis", "name": "Electronics Main"}
    mock_existing_category_orm = MagicMock(spec=CategoryOrm); mock_existing_category_orm.id = 123
    db_loaders_module.get_from_id_map.return_value = "123"
    mock_db_session_for_loaders.query(CategoryOrm).filter_by(id=123).first.return_value = mock_existing_category_orm
    returned_db_pk = load_category_to_db(mock_db_session_for_loaders, business_details_id, record_data, session_id, mock_redis_pipeline_for_loaders)
    assert returned_db_pk == 123
    db_loaders_module.add_to_id_map.assert_not_called()

def test_load_category_l2_parent_l1_exists_in_db_not_redis(mock_db_session_for_loaders, mock_redis_pipeline_for_loaders):
    business_details_id = 100; session_id = "sess_test_l2_parent_in_db"
    record_data = {"category_path": "Electronics/Wearables", "description": "Smart Watches"}
    db_loaders_module.get_from_id_map.return_value = None
    mock_l1 = MagicMock(spec=CategoryOrm); mock_l1.id = 1; mock_l1.name="Electronics"
    mock_db_session_for_loaders.query(CategoryOrm).filter_by().side_effect = [mock_l1, None]
    new_wearables_instance = None
    def capture_add_wearables(instance):
        nonlocal new_wearables_instance
        instance.id = 2
        new_wearables_instance = instance
    mock_db_session_for_loaders.add.side_effect = capture_add_wearables
    returned_db_pk = load_category_to_db(mock_db_session_for_loaders, business_details_id, record_data, session_id, mock_redis_pipeline_for_loaders)
    assert returned_db_pk == 2
    expected_add_redis_calls = [
        call(session_id, f"categories{DB_PK_MAP_SUFFIX}", "Electronics", 1, pipeline=mock_redis_pipeline_for_loaders),
        call(session_id, f"categories{DB_PK_MAP_SUFFIX}", "Electronics/Wearables", 2, pipeline=mock_redis_pipeline_for_loaders),
    ]
    db_loaders_module.add_to_id_map.assert_has_calls(expected_add_redis_calls, any_order=False)

def test_load_category_path_name_in_record_data_used_for_last_level(mock_db_session_for_loaders, mock_redis_pipeline_for_loaders):
    record_data = {"category_path": "Audio/Speakers", "name": "High-End Speakers", "description": "Premium Sound"}
    db_loaders_module.get_from_id_map.return_value = None
    mock_db_session_for_loaders.query(CategoryOrm).filter_by().first.return_value = None
    added_instances = {}
    current_mock_id_name_test = 0
    def capture_add_name_test(instance):
        nonlocal current_mock_id_name_test; current_mock_id_name_test += 1
        instance.id = current_mock_id_name_test; added_instances[instance.name] = instance
    mock_db_session_for_loaders.add.side_effect = capture_add_name_test
    returned_db_pk = load_category_to_db(mock_db_session_for_loaders, 100, record_data, "sess", mock_redis_pipeline_for_loaders)
    assert returned_db_pk == 2
    assert "High-End Speakers" in added_instances

def test_load_category_missing_category_path(mock_db_session_for_loaders, mock_redis_pipeline_for_loaders):
    record_data = {"name": "No Path", "description": "Test"}
    returned_db_pk = load_category_to_db(mock_db_session_for_loaders, 100, record_data, "sess", mock_redis_pipeline_for_loaders)
    assert returned_db_pk is None

@patch(f'{MODULE_PATH_FOR_REDIS_UTILS}.add_to_id_map')
def test_load_category_db_flush_error(mock_add_to_id_map_specific, mock_db_session_for_loaders, mock_redis_pipeline_for_loaders):
    record_data = {"category_path": "Electronics", "description": "Test"}
    db_loaders_module.get_from_id_map.return_value = None
    mock_db_session_for_loaders.query(CategoryOrm).filter_by().first.return_value = None
    mock_db_session_for_loaders.flush.side_effect = Exception("DB Flush Error")
    returned_db_pk = load_category_to_db(mock_db_session_for_loaders, 100, record_data, "sess", mock_redis_pipeline_for_loaders)
    assert returned_db_pk is None
    mock_add_to_id_map_specific.assert_not_called()


# --- Tests for load_brand_to_db (Bulk Operations) ---

@pytest.fixture
def sample_brand_records_for_bulk():
    return [
        {"name": "Brand New", "logo": "new.png", "active": "TRUE", "supplier_id": 1},
        {"name": "Brand Update", "logo": "update.png", "active": "FALSE"},
        {"name": "Brand Another New", "logo": "another.png", "active": "TRUE"},
    ]

def test_load_brand_bulk_all_new(mock_db_session_for_loaders, mock_redis_pipeline_for_loaders, sample_brand_records_for_bulk):
    business_details_id = 300
    session_id = "sess_brand_bulk_new"
    # Take 2 records that will be new
    new_records = [record for record in sample_brand_records_for_bulk if record["name"] != "Brand Update"]


    mock_db_session_for_loaders.query(BrandOrm).filter().all.return_value = [] # No existing brands

    def mock_bulk_insert_mappings_with_ids(orm_class, mappings):
        for i, mapping_dict in enumerate(mappings):
            mapping_dict['id'] = 1000 + i
    mock_db_session_for_loaders.bulk_insert_mappings.side_effect = mock_bulk_insert_mappings_with_ids

    summary = load_brand_to_db(mock_db_session_for_loaders, business_details_id, new_records, session_id, mock_redis_pipeline_for_loaders)

    assert summary == {"inserted": 2, "updated": 0, "errors": 0}
    mock_db_session_for_loaders.bulk_insert_mappings.assert_called_once_with(BrandOrm, ANY)
    assert len(mock_db_session_for_loaders.bulk_insert_mappings.call_args[0][1]) == 2
    mock_db_session_for_loaders.bulk_update_mappings.assert_not_called()

    expected_redis_calls = [
        call(session_id, f"brands{DB_PK_MAP_SUFFIX}", "Brand New", 1000, pipeline=mock_redis_pipeline_for_loaders),
        call(session_id, f"brands{DB_PK_MAP_SUFFIX}", "Brand Another New", 1001, pipeline=mock_redis_pipeline_for_loaders),
    ]
    db_loaders_module.add_to_id_map.assert_has_calls(expected_redis_calls, any_order=True)
    assert db_loaders_module.add_to_id_map.call_count == 2


def test_load_brand_bulk_all_updates(mock_db_session_for_loaders, mock_redis_pipeline_for_loaders, sample_brand_records_for_bulk):
    business_details_id = 300
    session_id = "sess_brand_bulk_update"

    existing_brand1 = BrandOrm(id=101, name="Brand New", business_details_id=business_details_id, logo="old_new.png")
    existing_brand2 = BrandOrm(id=102, name="Brand Update", business_details_id=business_details_id, logo="old_update.png")
    existing_brand3 = BrandOrm(id=103, name="Brand Another New", business_details_id=business_details_id, logo="old_another.png")
    mock_db_session_for_loaders.query(BrandOrm).filter().all.return_value = [existing_brand1, existing_brand2, existing_brand3]

    # Use all records from sample_brand_records_for_bulk as they all exist
    summary = load_brand_to_db(mock_db_session_for_loaders, business_details_id, sample_brand_records_for_bulk, session_id, mock_redis_pipeline_for_loaders)

    assert summary == {"inserted": 0, "updated": 3, "errors": 0}
    mock_db_session_for_loaders.bulk_update_mappings.assert_called_once_with(BrandOrm, ANY)

    passed_update_mappings = mock_db_session_for_loaders.bulk_update_mappings.call_args[0][1]
    assert len(passed_update_mappings) == 3
    assert any(m['id'] == 101 and m['name'] == 'Brand New' for m in passed_update_mappings)
    assert any(m['id'] == 102 and m['name'] == 'Brand Update' for m in passed_update_mappings)
    assert any(m['id'] == 103 and m['name'] == 'Brand Another New' for m in passed_update_mappings)

    mock_db_session_for_loaders.bulk_insert_mappings.assert_not_called()
    assert db_loaders_module.add_to_id_map.call_count == 3

def test_load_brand_bulk_mixed_insert_update(mock_db_session_for_loaders, mock_redis_pipeline_for_loaders, sample_brand_records_for_bulk):
    business_details_id = 300; session_id = "sess_brand_bulk_mixed"

    existing_brand_update = BrandOrm(id=202, name="Brand Update", business_details_id=business_details_id, logo="old_update.png")
    mock_db_session_for_loaders.query(BrandOrm).filter().all.return_value = [existing_brand_update]

    def mock_bulk_insert_mappings_mixed(orm_class, mappings):
        for i, mapping_dict in enumerate(mappings):
            mapping_dict['id'] = 3000 + i
    mock_db_session_for_loaders.bulk_insert_mappings.side_effect = mock_bulk_insert_mappings_mixed

    summary = load_brand_to_db(mock_db_session_for_loaders, business_details_id, sample_brand_records_for_bulk, session_id, mock_redis_pipeline_for_loaders)

    assert summary == {"inserted": 2, "updated": 1, "errors": 0}
    mock_db_session_for_loaders.bulk_insert_mappings.assert_called_once()
    mock_db_session_for_loaders.bulk_update_mappings.assert_called_once()
    assert db_loaders_module.add_to_id_map.call_count == 3


def test_load_brand_bulk_empty_list(mock_db_session_for_loaders, mock_redis_pipeline_for_loaders):
    summary = load_brand_to_db(mock_db_session_for_loaders, 300, [], "sess_empty", mock_redis_pipeline_for_loaders)
    assert summary == {"inserted": 0, "updated": 0, "errors": 0}

def test_load_brand_bulk_no_names_raises_error(mock_db_session_for_loaders, mock_redis_pipeline_for_loaders):
    records = [{"logo": "logo1.png"}, {"logo": "logo2.png"}]
    with pytest.raises(DataLoaderError) as excinfo:
        load_brand_to_db(mock_db_session_for_loaders, 300, records, "sess_no_names", mock_redis_pipeline_for_loaders)
    assert excinfo.value.error_type == ErrorType.VALIDATION

def test_load_brand_bulk_insert_integrity_error(mock_db_session_for_loaders, mock_redis_pipeline_for_loaders, sample_brand_records_for_bulk):
    mock_db_session_for_loaders.query(BrandOrm).filter().all.return_value = []
    mock_db_session_for_loaders.bulk_insert_mappings.side_effect = IntegrityError("mock_bulk_insert", params={}, orig=Exception("Unique constraint failed"))

    with pytest.raises(DataLoaderError) as excinfo:
        load_brand_to_db(mock_db_session_for_loaders, 300, sample_brand_records_for_bulk, "sess_integrity", mock_redis_pipeline_for_loaders)
    assert excinfo.value.error_type == ErrorType.DATABASE

def test_load_brand_bulk_update_general_error(mock_db_session_for_loaders, mock_redis_pipeline_for_loaders, sample_brand_records_for_bulk):
    existing_brand = BrandOrm(id=101, name="Brand Update", business_details_id=300)
    mock_db_session_for_loaders.query(BrandOrm).filter().all.return_value = [existing_brand]
    mock_db_session_for_loaders.bulk_update_mappings.side_effect = Exception("Something broke in bulk update")
    records_to_update = [r for r in sample_brand_records_for_bulk if r["name"] == "Brand Update"]
    with pytest.raises(DataLoaderError) as excinfo:
        load_brand_to_db(mock_db_session_for_loaders, 300, records_to_update, "sess_update_fail", mock_redis_pipeline_for_loaders)
    assert excinfo.value.error_type == ErrorType.UNEXPECTED_ROW_ERROR

def test_load_brand_bulk_id_not_populated_error_in_summary(mock_db_session_for_loaders, mock_redis_pipeline_for_loaders):
    new_records = [{"name": "Brand X", "logo": "x.png"}]
    mock_db_session_for_loaders.query(BrandOrm).filter().all.return_value = []
    def mock_bulk_insert_no_id_populate(orm_class, mappings): pass
    mock_db_session_for_loaders.bulk_insert_mappings.side_effect = mock_bulk_insert_no_id_populate

    summary = load_brand_to_db(mock_db_session_for_loaders, 300, new_records, "sess_id_not_pop", mock_redis_pipeline_for_loaders)
    assert summary["inserted"] == 1
    assert summary["errors"] == 1
    # Verify add_to_id_map was not called for 'Brand X' because its ID was missing
    # This check needs careful implementation if other brands *were* successfully mapped in the same call
    # For this specific test case where only Brand X is processed, it's simpler:
    if db_loaders_module.add_to_id_map.call_args_list: # Check if it was called at all
        for call_obj in db_loaders_module.add_to_id_map.call_args_list:
            args, _ = call_obj
            assert args[2] != "Brand X" # args[2] is the brand_name
    else:
        db_loaders_module.add_to_id_map.assert_not_called() # If no brands got IDs


# --- Tests for load_return_policy_to_db ---
class TestLoadReturnPolicyToDb:

    def test_load_return_policy_new_sales_return_allowed(self, mock_db_session_for_loaders, mock_redis_pipeline_for_loaders):
        business_details_id = 1
        session_id = "sess_rp_new_allowed"
        created_at_csv = datetime(2023, 1, 1, 12, 0, 0)
        record_data = {
            "id": None,
            "return_policy_type": "SALES_RETURN_ALLOWED",
            "policy_name": "14 Day Returns",
            "time_period_return": 14,
            "grace_period_return": 2,
            "business_details_id": business_details_id,
            "created_date": created_at_csv
        }
        mock_db_session_for_loaders.query(ReturnPolicyOrm).filter_by().first.return_value = None

        added_instance_ref = {}
        def capture_instance(instance):
            instance.id = 123
            added_instance_ref['instance'] = instance
        mock_db_session_for_loaders.add.side_effect = capture_instance

        returned_pk = load_return_policy_to_db(
            mock_db_session_for_loaders, business_details_id, record_data, session_id, mock_redis_pipeline_for_loaders
        )
        assert returned_pk == 123
        mock_db_session_for_loaders.add.assert_called_once()
        added_policy = added_instance_ref['instance']
        assert added_policy.policy_name == "14 Day Returns"
        assert added_policy.return_type == "SALES_RETURN_ALLOWED"
        assert added_policy.return_days == 14
        # assert added_policy.grace_period_return == 2 # No grace_period_return on ORM
        assert added_policy.business_details_id == business_details_id
        assert added_policy.created_date_ts == created_at_csv
        mock_db_session_for_loaders.flush.assert_called_once()
        db_loaders_module.add_to_id_map.assert_not_called()

    def test_load_return_policy_new_sales_are_final(self, mock_db_session_for_loaders, mock_redis_pipeline_for_loaders):
        business_details_id = 1
        session_id = "sess_rp_new_final"
        record_data = {
            "id": None,
            "return_policy_type": "SALES_ARE_FINAL",
            "policy_name": "Final Sale Item",
            "time_period_return": 10,
            "grace_period_return": 5,
            "business_details_id": business_details_id
        }
        mock_db_session_for_loaders.query(ReturnPolicyOrm).filter_by().first.return_value = None
        added_instance_ref = {}
        def capture_instance(instance): instance.id = 124; added_instance_ref['instance'] = instance
        mock_db_session_for_loaders.add.side_effect = capture_instance

        returned_pk = load_return_policy_to_db(
            mock_db_session_for_loaders, business_details_id, record_data, session_id, mock_redis_pipeline_for_loaders
        )
        assert returned_pk == 124
        mock_db_session_for_loaders.add.assert_called_once()
        added_policy = added_instance_ref['instance']
        assert added_policy.return_type == "SALES_ARE_FINAL"
        assert added_policy.policy_name is None
        assert added_policy.return_days is None
        # assert added_policy.grace_period_return is None # No grace_period_return on ORM
        mock_db_session_for_loaders.flush.assert_called_once()
        db_loaders_module.add_to_id_map.assert_not_called()

    def test_load_return_policy_update_existing_by_csv_id(self, mock_db_session_for_loaders, mock_redis_pipeline_for_loaders):
        business_details_id = 1
        session_id = "sess_rp_update_existing"
        csv_id = 456
        updated_at_csv = datetime(2023, 1, 3, 0, 0, 0)
        record_data = {
            "id": csv_id,
            "return_policy_type": "SALES_RETURN_ALLOWED",
            "policy_name": "30 Day Super Returns",
            "time_period_return": 30,
            "updated_date": updated_at_csv,
            "business_details_id": business_details_id
        }
        mock_existing_policy = ReturnPolicyOrm(
            id=csv_id, business_details_id=business_details_id, policy_name="Old Name",
            return_type="SALES_RETURN_ALLOWED", return_days=10,
            created_date_ts=datetime(2022,1,1)
        )
        mock_db_session_for_loaders.query(ReturnPolicyOrm).filter_by(id=csv_id, business_details_id=business_details_id).first.return_value = mock_existing_policy

        returned_pk = load_return_policy_to_db(
            mock_db_session_for_loaders, business_details_id, record_data, session_id, mock_redis_pipeline_for_loaders
        )
        assert returned_pk == csv_id
        mock_db_session_for_loaders.add.assert_not_called()
        assert mock_existing_policy.policy_name == "30 Day Super Returns"
        assert mock_existing_policy.return_days == 30
        assert mock_existing_policy.updated_date_ts == updated_at_csv
        db_loaders_module.add_to_id_map.assert_not_called()

    def test_load_return_policy_update_sales_are_final_nulls_fields(self, mock_db_session_for_loaders, mock_redis_pipeline_for_loaders):
        business_details_id = 1; session_id = "sess_rp_update_to_final"
        csv_id = 789
        record_data = {
            "id": csv_id, "return_policy_type": "SALES_ARE_FINAL",
            "policy_name": "This policy name should be nulled by loader",
            "time_period_return": 5,
            "business_details_id": business_details_id
        }
        mock_existing_policy = ReturnPolicyOrm(
            id=csv_id, business_details_id=business_details_id, policy_name="Returnable Policy",
            return_type="SALES_RETURN_ALLOWED", return_days=10
        )
        mock_db_session_for_loaders.query(ReturnPolicyOrm).filter_by(id=csv_id, business_details_id=business_details_id).first.return_value = mock_existing_policy

        returned_pk = load_return_policy_to_db(mock_db_session_for_loaders, business_details_id, record_data, session_id, mock_redis_pipeline_for_loaders)
        assert returned_pk == csv_id
        assert mock_existing_policy.return_type == "SALES_ARE_FINAL"
        assert mock_existing_policy.policy_name is None
        assert mock_existing_policy.return_days is None
        # assert mock_existing_policy.grace_period_return is None # No field

    def test_load_return_policy_csv_id_not_found_creates_new(self, mock_db_session_for_loaders, mock_redis_pipeline_for_loaders):
        business_details_id = 1; session_id = "sess_rp_id_not_found"
        record_data = {"id": 999, "return_policy_type": "SALES_ARE_FINAL", "business_details_id": business_details_id}
        mock_db_session_for_loaders.query(ReturnPolicyOrm).filter_by(id=999, business_details_id=business_details_id).first.return_value = None

        added_instance_ref = {}
        def capture_instance(instance): instance.id = 125; added_instance_ref['instance'] = instance
        mock_db_session_for_loaders.add.side_effect = capture_instance

        returned_pk = load_return_policy_to_db(mock_db_session_for_loaders, business_details_id, record_data, session_id, mock_redis_pipeline_for_loaders)
        assert returned_pk == 125
        mock_db_session_for_loaders.add.assert_called_once()
        added_policy = added_instance_ref['instance']
        assert added_policy.return_type == "SALES_ARE_FINAL"
        assert added_policy.policy_name is None
        assert added_policy.id != 999

    def test_load_return_policy_timestamps_from_csv(self, mock_db_session_for_loaders, mock_redis_pipeline_for_loaders):
        business_details_id = 1; session_id = "sess_rp_timestamps"
        csv_created_date = datetime(2023, 1, 1, 10, 0, 0)
        csv_updated_date = datetime(2023, 1, 2, 11, 0, 0)
        record_data = {
            "id": None, "return_policy_type": "SALES_RETURN_ALLOWED",
            "policy_name": "Policy with CSV Dates", "time_period_return": 3,
            "created_date": csv_created_date, "updated_date": csv_updated_date,
            "business_details_id": business_details_id
        }
        mock_db_session_for_loaders.query(ReturnPolicyOrm).filter_by().first.return_value = None
        added_instance_ref = {}
        def capture_instance(instance): instance.id = 126; added_instance_ref['instance'] = instance
        mock_db_session_for_loaders.add.side_effect = capture_instance

        load_return_policy_to_db(mock_db_session_for_loaders, business_details_id, record_data, session_id, mock_redis_pipeline_for_loaders)
        added_policy = added_instance_ref['instance']
        assert added_policy.created_date_ts == csv_created_date # Check against created_date_ts
        assert added_policy.updated_date_ts == csv_updated_date # Check against updated_date_ts

    def test_load_return_policy_missing_return_policy_type_returns_none(self, mock_db_session_for_loaders, mock_redis_pipeline_for_loaders):
        record_data = {"policy_name": "Policy without type"}
        returned_pk = load_return_policy_to_db(mock_db_session_for_loaders, 1, record_data, "sess_no_type", mock_redis_pipeline_for_loaders)
        assert returned_pk is None
        mock_db_session_for_loaders.add.assert_not_called()

    def test_load_return_policy_db_flush_error_returns_none(self, mock_db_session_for_loaders, mock_redis_pipeline_for_loaders):
        record_data = {"id": None, "return_policy_type": "SALES_ARE_FINAL", "business_details_id": 1}
        mock_db_session_for_loaders.query(ReturnPolicyOrm).filter_by().first.return_value = None
        mock_db_session_for_loaders.flush.side_effect = SQLAlchemyError("Simulated DB Flush Error")

        returned_pk = load_return_policy_to_db(mock_db_session_for_loaders, 1, record_data, "sess_flush_err", mock_redis_pipeline_for_loaders)
        assert returned_pk is None
        mock_db_session_for_loaders.add.assert_called_once()
        db_loaders_module.add_to_id_map.assert_not_called()
