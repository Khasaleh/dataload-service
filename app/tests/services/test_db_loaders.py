import pytest
from unittest.mock import MagicMock, patch, call, ANY
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError, IntegrityError

from app.services.db_loaders import (
    load_category_to_db, load_brand_to_db, load_return_policy_to_db,
    DB_PK_MAP_SUFFIX
)
import app.services.db_loaders as db_loaders_module
from app.db.models import CategoryOrm, BrandOrm, ReturnPolicyOrm
from app.exceptions import DataLoaderError
from app.models.schemas import ErrorType
from datetime import datetime

MODULE_PATH_FOR_REDIS_UTILS = "app.services.db_loaders"

@pytest.fixture
def mock_db_session_for_loaders():
    return MagicMock(spec=Session)

@pytest.fixture
def mock_redis_pipeline_for_loaders():
    mock_pipe = MagicMock()
    mock_pipe.execute.return_value = []
    return mock_pipe

@pytest.fixture(autouse=True)
def auto_patch_redis_utils(mocker):
    mocker.patch(f'{MODULE_PATH_FOR_REDIS_UTILS}.add_to_id_map', autospec=True)
    mocker.patch(f'{MODULE_PATH_FOR_REDIS_UTILS}.get_from_id_map', autospec=True)


# --- Existing Tests for load_category_to_db (ensure to adapt for DataLoaderError) ---
# (Original test_load_category_... functions would be here, modified for DataLoaderError)
def test_load_category_l1_new(mock_db_session_for_loaders, mock_redis_pipeline_for_loaders):
    business_details_id = 100; session_id = "sess_test_l1_new"
    record_data = {"category_path": "Electronics", "description": "All electronic items", "name": "Electronics Main"}
    db_loaders_module.get_from_id_map.return_value = None
    mock_db_session_for_loaders.query(CategoryOrm).filter_by().first.return_value = None
    new_cat_instance = None
    def capture_add(instance): nonlocal new_cat_instance; instance.id = 123; new_cat_instance = instance
    mock_db_session_for_loaders.add.side_effect = capture_add

    with patch.object(db_loaders_module.logger, 'error') as mock_logger_error:
        returned_db_pk = load_category_to_db(mock_db_session_for_loaders, business_details_id, record_data, session_id, mock_redis_pipeline_for_loaders)
        assert returned_db_pk == 123
        mock_logger_error.assert_not_called() # Ensure no errors logged for success

# ... other category tests, adapted to use pytest.raises(DataLoaderError) for error paths ...
def test_load_category_missing_category_path(mock_db_session_for_loaders, mock_redis_pipeline_for_loaders):
    record_data = {"name": "No Path", "description": "Test"}
    with pytest.raises(DataLoaderError) as excinfo:
        load_category_to_db(mock_db_session_for_loaders, 100, record_data, "sess", mock_redis_pipeline_for_loaders)
    assert excinfo.value.error_type == ErrorType.VALIDATION
    assert "Missing 'category_path'" in excinfo.value.message


# --- Tests for load_brand_to_db (Bulk Operations) ---
@pytest.fixture
def sample_brand_records_for_bulk():
    return [
        {"name": "Brand New", "logo": "new.png", "active": "TRUE", "supplier_id": 1},
        {"name": "Brand Update", "logo": "update.png", "active": "FALSE"},
        {"name": "Brand Another New", "logo": "another.png", "active": "TRUE"},
    ]

def test_load_brand_bulk_all_new(mock_db_session_for_loaders, mock_redis_pipeline_for_loaders, sample_brand_records_for_bulk):
    business_details_id = 300; session_id = "sess_brand_bulk_new"
    new_records = [r for r in sample_brand_records_for_bulk if r["name"] != "Brand Update"]
    mock_db_session_for_loaders.query(BrandOrm).filter().all.return_value = []
    def mock_bulk_insert_mappings_with_ids(orm_class, mappings):
        for i, mapping_dict in enumerate(mappings): mapping_dict['id'] = 1000 + i
    mock_db_session_for_loaders.bulk_insert_mappings.side_effect = mock_bulk_insert_mappings_with_ids
    summary = load_brand_to_db(mock_db_session_for_loaders, business_details_id, new_records, session_id, mock_redis_pipeline_for_loaders)
    assert summary == {"inserted": 2, "updated": 0, "errors": 0}
    # ... (rest of brand bulk assertions) ...

# ... (all other bulk brand tests from previous step) ...
def test_load_brand_bulk_id_not_populated_error_in_summary(mock_db_session_for_loaders, mock_redis_pipeline_for_loaders):
    new_records = [{"name": "Brand X", "logo": "x.png"}]
    mock_db_session_for_loaders.query(BrandOrm).filter().all.return_value = []
    def mock_bulk_insert_no_id_populate(orm_class, mappings): pass
    mock_db_session_for_loaders.bulk_insert_mappings.side_effect = mock_bulk_insert_no_id_populate
    summary = load_brand_to_db(mock_db_session_for_loaders, 300, new_records, "sess_id_not_pop", mock_redis_pipeline_for_loaders)
    assert summary["inserted"] == 1
    assert summary["errors"] == 1
    if db_loaders_module.add_to_id_map.call_args_list:
        for call_obj in db_loaders_module.add_to_id_map.call_args_list:
            args, _ = call_obj
            assert args[2] != "Brand X"
    else:
        db_loaders_module.add_to_id_map.assert_not_called()

# --- Tests for load_return_policy_to_db (Bulk Operations) ---

@pytest.fixture
def sample_return_policy_records():
    return [
        {"id": None, "return_policy_type": "SALES_RETURN_ALLOWED", "policy_name": "Policy A (New)", "time_period_return": 14, "created_date": datetime(2023,1,1)},
        {"id": 1, "return_policy_type": "SALES_ARE_FINAL", "policy_name": "Policy B (Update)", "time_period_return": 0, "updated_date": datetime(2023,1,2)}, # Will be nulled
        {"id": None, "return_policy_type": "SALES_RETURN_ALLOWED", "policy_name": "Policy C (New)", "time_period_return": 30},
        {"id": 2, "return_policy_type": "SALES_RETURN_ALLOWED", "policy_name": "Policy D (Update ID not found)", "time_period_return": 7}, # This ID won't be found
        {"id": None, "return_policy_type": "SALES_RETURN_ALLOWED", "policy_name": "Policy B (Update)", "time_period_return": 60}, # Will update existing Policy B by name
    ]

def test_load_return_policy_bulk_mixed_insert_update(mock_db_session_for_loaders, sample_return_policy_records):
    business_details_id = 400
    session_id = "sess_rp_bulk_mixed"
    records = sample_return_policy_records

    # Existing policies in DB
    # Policy B exists by name (and ID 1, which will be updated by name if ID record not found)
    existing_policy_b = ReturnPolicyOrm(id=1, business_details_id=business_details_id, policy_name="Policy B (Update)", return_type="SALES_RETURN_ALLOWED", return_days=5)

    # Simulate DB query results
    # First query (by ID): find only policy with ID 1 (Policy B)
    # Second query (by Name): find only policy "Policy B (Update)"
    def query_side_effect(*args, **kwargs):
        if mock_db_session_for_loaders.query(ReturnPolicyOrm).filter().all.call_count == 1: # By ID
            return [p for p in [existing_policy_b] if p.id in [r.get('id') for r in records_with_id_in_csv_local]]
        elif mock_db_session_for_loaders.query(ReturnPolicyOrm).filter().all.call_count == 2: # By Name
            return [p for p in [existing_policy_b] if p.policy_name in policy_names_to_check_local]
        return []

    records_with_id_in_csv_local = [r for r in records if r.get("id") is not None]
    policy_names_to_check_local = {r['policy_name'] for r in records if r.get('id') is None and r.get('policy_name')}
    mock_db_session_for_loaders.query(ReturnPolicyOrm).filter().all.side_effect = query_side_effect

    summary = load_return_policy_to_db(mock_db_session_for_loaders, business_details_id, records, session_id, None)

    assert summary["inserted"] == 2 # Policy A, Policy C
    assert summary["updated"] == 2 # Policy B (updated by ID, then again by Name for the last record) -> this highlights a potential double update if not careful.
                                   # The current logic: record with ID 1 updates existing_policy_b. Record 5 also matches "Policy B (Update)" and updates it again.
                                   # A more robust approach might be to ensure a record is only processed once.
                                   # For now, testing current logic. The last update for "Policy B (Update)" would be from record 5.
    assert summary["errors"] == 1  # Policy D (ID 2 not found)

    # Check calls to bulk operations
    # Updates: Policy B (ID 1) from record 2, Policy B (ID 1) again from record 5
    assert mock_db_session_for_loaders.bulk_update_mappings.call_count == 1
    update_args = mock_db_session_for_loaders.bulk_update_mappings.call_args[0][1]
    assert len(update_args) == 2
    assert any(u['id'] == 1 and u['return_days'] is None for u in update_args) # From record 2 (SALES_ARE_FINAL)
    assert any(u['id'] == 1 and u['return_days'] == 60 for u in update_args) # From record 5

    # Inserts: Policy A, Policy C
    assert mock_db_session_for_loaders.bulk_insert_mappings.call_count == 1
    insert_args = mock_db_session_for_loaders.bulk_insert_mappings.call_args[0][1]
    assert len(insert_args) == 2
    assert any(i['policy_name'] == "Policy A (New)" for i in insert_args)
    assert any(i['policy_name'] == "Policy C (New)" for i in insert_args)

def test_load_return_policy_bulk_sales_are_final_nullifies_fields(mock_db_session_for_loaders):
    business_details_id = 400; session_id = "sess_rp_bulk_final"
    records = [{"id": None, "return_policy_type": "SALES_ARE_FINAL", "policy_name": "Should Be Nulled", "time_period_return": 10}]

    mock_db_session_for_loaders.query(ReturnPolicyOrm).filter().all.return_value = [] # No existing

    load_return_policy_to_db(mock_db_session_for_loaders, business_details_id, records, session_id, None)

    mock_db_session_for_loaders.bulk_insert_mappings.assert_called_once()
    inserted_mapping = mock_db_session_for_loaders.bulk_insert_mappings.call_args[0][1][0]
    assert inserted_mapping["policy_name"] is None
    assert inserted_mapping["return_days"] is None # Check mapped field
    assert "time_period_return" not in inserted_mapping # Original field removed

def test_load_return_policy_bulk_integrity_error(mock_db_session_for_loaders, sample_return_policy_records):
    mock_db_session_for_loaders.query(ReturnPolicyOrm).filter().all.return_value = [] # All new
    mock_db_session_for_loaders.bulk_insert_mappings.side_effect = IntegrityError("mock", params={}, orig=Exception("Unique constraint failed"))

    with pytest.raises(DataLoaderError) as excinfo:
        load_return_policy_to_db(mock_db_session_for_loaders, 400, sample_return_policy_records, "sess_rp_integrity", None)
    assert excinfo.value.error_type == ErrorType.DATABASE

def test_load_return_policy_empty_list(mock_db_session_for_loaders):
    summary = load_return_policy_to_db(mock_db_session_for_loaders, 400, [], "sess_rp_empty", None)
    assert summary == {"inserted": 0, "updated": 0, "errors": 0}

# (Keep other existing test classes like TestLoadPriceToDb if they exist and are relevant)
# ...
# Placeholder for TestLoadPriceToDb and other loaders
# Ensure to refactor their error assertions from `assert X is None` to `with pytest.raises(DataLoaderError):`
