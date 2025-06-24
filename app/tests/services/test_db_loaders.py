import pytest
from unittest.mock import MagicMock, patch, call, ANY
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError, IntegrityError

# Import ORM models first
from app.db.models import (
    CategoryOrm, BrandOrm, ReturnPolicyOrm, PriceOrm, ProductOrm, ProductItemOrm
)
# Then import functions to test and other utilities
from app.services.db_loaders import (
    load_category_to_db, load_brand_to_db, load_return_policy_to_db, load_price_to_db, # Added load_price_to_db
    DB_PK_MAP_SUFFIX
)
import app.services.db_loaders as db_loaders_module # For mocking items within the module
from app.exceptions import DataLoaderError
from app.models.schemas import ErrorType
from app.dataload.models.price_csv import PriceTypeEnum # For Price tests
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
def test_load_category_l1_new(mock_db_session_for_loaders, mock_redis_pipeline_for_loaders):
    business_details_id = 100; session_id = "sess_test_l1_new"
    record_data = {"category_path": "Electronics", "description": "All electronic items", "name": "Electronics Main"}
    db_loaders_module.get_from_id_map.return_value = None
    mock_db_session_for_loaders.query(CategoryOrm).filter_by().first.return_value = None
    new_cat_instance = None
    def capture_add(instance): nonlocal new_cat_instance; instance.id = 123; new_cat_instance = instance
    mock_db_session_for_loaders.add.side_effect = capture_add

    with patch.object(db_loaders_module.logger, 'error') as mock_logger_error: # Keep logger mock for this test if it checks logs
        returned_db_pk = load_category_to_db(mock_db_session_for_loaders, business_details_id, record_data, session_id, mock_redis_pipeline_for_loaders)
        assert returned_db_pk == 123
        mock_logger_error.assert_not_called()

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
    # ... (other assertions as before)

# ... (all other bulk brand tests from previous step, ensure they pass or adapt) ...
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
        {"id": 1, "return_policy_type": "SALES_ARE_FINAL", "policy_name": "Policy B (Update)", "time_period_return": 0, "updated_date": datetime(2023,1,2)},
        {"id": None, "return_policy_type": "SALES_RETURN_ALLOWED", "policy_name": "Policy C (New)", "time_period_return": 30},
        {"id": 2, "return_policy_type": "SALES_RETURN_ALLOWED", "policy_name": "Policy D (Update ID not found)", "time_period_return": 7},
        {"id": None, "return_policy_type": "SALES_RETURN_ALLOWED", "policy_name": "Policy B (Update)", "time_period_return": 60},
    ]

def test_load_return_policy_bulk_mixed_insert_update(mock_db_session_for_loaders, sample_return_policy_records):
    business_details_id = 400; session_id = "sess_rp_bulk_mixed"
    records = sample_return_policy_records
    existing_policy_b = ReturnPolicyOrm(id=1, business_details_id=business_details_id, policy_name="Policy B (Update)", return_type="SALES_RETURN_ALLOWED", return_days=5)

    def query_side_effect(*args, **kwargs):
        # Simplified mock: first call (by ID) returns existing_policy_b if ID is 1. Second call (by name) returns existing_policy_b if name is "Policy B (Update)".
        current_call_count = mock_db_session_for_loaders.query(ReturnPolicyOrm).filter().all.call_count
        if current_call_count == 1: # By ID
            ids_being_queried = kwargs.get('synchronize_session', {}).get('id', []) # Heuristic
            if isinstance(args[0], sqlalchemy.sql.elements.BinaryExpression) and args[0].left.name == 'id': # A bit fragile
                 ids_being_queried = [c.value for c in args[0].right.clauses]


            return [p for p in [existing_policy_b] if p.id in ids_from_csv_local_ref[0]]
        elif current_call_count == 2: # By Name
            names_being_queried = []
            if isinstance(args[0], sqlalchemy.sql.elements.BinaryExpression) and args[0].left.name == 'policy_name':
                 names_being_queried = [c.value for c in args[0].right.clauses]
            return [p for p in [existing_policy_b] if p.policy_name in names_being_queried]
        return []

    ids_from_csv_local_ref = [[r.get('id') for r in records if r.get("id") is not None]] # Closure workaround
    mock_db_session_for_loaders.query(ReturnPolicyOrm).filter().all.side_effect = query_side_effect

    summary = load_return_policy_to_db(mock_db_session_for_loaders, business_details_id, records, session_id, None)

    assert summary["inserted"] == 2
    assert summary["updated"] == 1 # Policy B should only be updated once effectively by name or by ID.
    assert summary["errors"] == 1
    # ... (rest of assertions for return policy bulk) ...

# ... (other return policy bulk tests) ...
def test_load_return_policy_empty_list(mock_db_session_for_loaders):
    summary = load_return_policy_to_db(mock_db_session_for_loaders, 400, [], "sess_rp_empty", None)
    assert summary == {"inserted": 0, "updated": 0, "errors": 0}


# --- Tests for load_price_to_db (Bulk Operations) ---
@pytest.fixture
def sample_price_records():
    return [
        # Product Prices
        {"price_type": "PRODUCT", "product_id": "101", "price": 19.99, "currency": "USD"}, # New
        {"price_type": "PRODUCT", "product_id": "102", "price": 29.99, "discount_price": 25.00}, # Update
        {"price_type": "PRODUCT", "product_id": "999", "price": 9.99}, # Invalid product_id
        # SKU Prices
        {"price_type": "SKU", "sku_id": "201", "price": 9.99}, # New
        {"price_type": "SKU", "sku_id": "202", "price": 14.99, "cost_price": 5.00}, # Update
        {"price_type": "SKU", "sku_id": "888", "price": 4.99}, # Invalid sku_id
        # Invalid type
        {"price_type": "INVALID_TYPE", "product_id": "103", "price": 1.00},
    ]

class TestLoadPriceToDbBulk:
    def test_load_price_bulk_mixed_new_update_errors(self, mock_db_session_for_loaders, sample_price_records):
        business_details_id = 500
        session_id = "sess_price_bulk_mixed"
        records = sample_price_records

        # Mock valid products and SKUs
        mock_db_session_for_loaders.query(ProductOrm.id).filter().all.return_value = [(101,), (102,)] # Valid product IDs
        mock_db_session_for_loaders.query(ProductItemOrm.id).filter().all.return_value = [(201,), (202,)] # Valid SKU IDs

        # Mock existing prices
        existing_prod_price = PriceOrm(id=1, product_id=102, business_details_id=business_details_id, price=30.00)
        existing_sku_price = PriceOrm(id=2, sku_id=202, business_details_id=business_details_id, price=15.00)

        def mock_price_query(*args, **kwargs):
            # This is a simplified mock. A real one would inspect filter conditions more closely.
            # For product prices query
            if mock_db_session_for_loaders.query(PriceOrm).filter().all.call_count == 1:
                 return [existing_prod_price] if valid_db_product_ids_ref[0] and 102 in valid_db_product_ids_ref[0] else []
            # For SKU prices query
            elif mock_db_session_for_loaders.query(PriceOrm).filter().all.call_count == 2:
                 return [existing_sku_price] if valid_db_sku_ids_ref[0] and 202 in valid_db_sku_ids_ref[0] else []
            return []

        valid_db_product_ids_ref = [None] # Using list for closure modification
        valid_db_sku_ids_ref = [None]

        original_product_query = mock_db_session_for_loaders.query(ProductOrm.id).filter().all
        def product_query_side_effect(*args, **kwargs):
            res = original_product_query(*args, **kwargs)
            valid_db_product_ids_ref[0] = {item[0] for item in res}
            return res
        mock_db_session_for_loaders.query(ProductOrm.id).filter().all.side_effect = product_query_side_effect

        original_sku_query = mock_db_session_for_loaders.query(ProductItemOrm.id).filter().all
        def sku_query_side_effect(*args, **kwargs):
            res = original_sku_query(*args, **kwargs)
            valid_db_sku_ids_ref[0] = {item[0] for item in res}
            return res
        mock_db_session_for_loaders.query(ProductItemOrm.id).filter().all.side_effect = sku_query_side_effect

        mock_db_session_for_loaders.query(PriceOrm).filter().all.side_effect = mock_price_query


        summary = load_price_to_db(mock_db_session_for_loaders, business_details_id, records, session_id, None)

        assert summary["inserted"] == 2 # Product 101, SKU 201
        assert summary["updated"] == 2 # Product 102, SKU 202
        assert summary["errors"] == 3  # Product 999 (invalid), SKU 888 (invalid), INVALID_TYPE

        # Check bulk_insert_mappings
        mock_db_session_for_loaders.bulk_insert_mappings.assert_called_once_with(PriceOrm, ANY)
        insert_args = mock_db_session_for_loaders.bulk_insert_mappings.call_args[0][1]
        assert len(insert_args) == 2
        assert any(d.get('product_id') == 101 and d.get('price') == 19.99 for d in insert_args)
        assert any(d.get('sku_id') == 201 and d.get('price') == 9.99 for d in insert_args)

        # Check bulk_update_mappings
        mock_db_session_for_loaders.bulk_update_mappings.assert_called_once_with(PriceOrm, ANY)
        update_args = mock_db_session_for_loaders.bulk_update_mappings.call_args[0][1]
        assert len(update_args) == 2
        assert any(d.get('id') == 1 and d.get('product_id') == 102 and d.get('price') == 29.99 for d in update_args)
        assert any(d.get('id') == 2 and d.get('sku_id') == 202 and d.get('price') == 14.99 for d in update_args)

    def test_load_price_bulk_db_integrity_error(self, mock_db_session_for_loaders):
        business_details_id = 500; session_id = "sess_price_integrity"
        records = [{"price_type": "PRODUCT", "product_id": "101", "price": 10.00}]
        mock_db_session_for_loaders.query(ProductOrm.id).filter().all.return_value = [(101,)]
        mock_db_session_for_loaders.query(PriceOrm).filter().all.return_value = [] # No existing prices
        mock_db_session_for_loaders.bulk_insert_mappings.side_effect = IntegrityError("mock", params={}, orig=Exception("Unique constraint"))

        with pytest.raises(DataLoaderError) as excinfo:
            load_price_to_db(mock_db_session_for_loaders, business_details_id, records, session_id, None)
        assert excinfo.value.error_type == ErrorType.DATABASE

    def test_load_price_bulk_empty_list(self, mock_db_session_for_loaders):
        summary = load_price_to_db(mock_db_session_for_loaders, 500, [], "sess_price_empty", None)
        assert summary == {"inserted": 0, "updated": 0, "errors": 0}
        mock_db_session_for_loaders.bulk_insert_mappings.assert_not_called()
        mock_db_session_for_loaders.bulk_update_mappings.assert_not_called()

    def test_load_price_invalid_target_ids_counted_as_errors(self, mock_db_session_for_loaders):
        business_details_id = 500
        records = [
            {"price_type": "PRODUCT", "product_id": "999", "price": 9.99}, # Invalid product_id
            {"price_type": "SKU", "sku_id": "888", "price": 4.99}, # Invalid sku_id
        ]
        mock_db_session_for_loaders.query(ProductOrm.id).filter().all.return_value = [] # No valid products
        mock_db_session_for_loaders.query(ProductItemOrm.id).filter().all.return_value = [] # No valid SKUs
        mock_db_session_for_loaders.query(PriceOrm).filter().all.return_value = []


        summary = load_price_to_db(mock_db_session_for_loaders, business_details_id, records, "sess_price_invalid_targets", None)
        assert summary["inserted"] == 0
        assert summary["updated"] == 0
        assert summary["errors"] == 2
        mock_db_session_for_loaders.bulk_insert_mappings.assert_not_called()
        mock_db_session_for_loaders.bulk_update_mappings.assert_not_called()
