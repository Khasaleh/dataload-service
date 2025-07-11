import pytest
import itertools # Added for mock_id_counter
from unittest.mock import MagicMock, patch, call, ANY
from typing import List, Dict, Any, Optional

from sqlalchemy.orm import Session
from pydantic import ValidationError

from app.dataload.models.item_csv import ItemCsvModel
from app.dataload.item_loader import (
    load_item_record_to_db,
    load_items_to_db,
    # _lookup_attribute_ids, # Tested via load_item_record_to_db
    # _lookup_attribute_value_ids # Tested via load_item_record_to_db
)
from app.db.models import ( 
    ProductOrm, SkuOrm, MainSkuOrm, ProductVariantOrm, ProductImageOrm,
    AttributeOrm, AttributeValueOrm, BusinessDetailsOrm # Added BusinessDetailsOrm for completeness if needed
)
from app.dataload.parsers.item_parser import ItemParserError # Imported directly
from app.exceptions import DataLoaderError
from app.models.schemas import ErrorType


# --- Fixtures ---

@pytest.fixture
def mock_db_session():
    session = MagicMock(spec=Session)
    mock_query_obj = MagicMock()
    session.query.return_value = mock_query_obj
    mock_query_obj.filter.return_value = mock_query_obj
    mock_query_obj.filter_by.return_value = mock_query_obj
    
    session.add = MagicMock()
    session.flush = MagicMock()
    
    mock_savepoint = MagicMock()
    session.begin_nested.return_value = mock_savepoint # For load_items_to_db
    # savepoint's commit/rollback are part of the mock_savepoint object already
    # For load_item_record_to_db, it might not use nested if called directly
    # but load_items_to_db test will verify savepoint usage.
    return session

@pytest.fixture
def sample_item_csv_row_dict() -> Dict[str, Any]:
    return {
        "product_name": "Test Product Alpha",
        "attributes": "color|main_attribute:true|size|main_attribute:false",
        "attribute_combination": "{Black|main_sku:true:White|main_sku:false}|{S:M}",
        "price": "10.00:12.00|20.00:22.00",
        "quantity": "100:110|200:210",
        "status": "ACTIVE|INACTIVE", 
        "order_limit": "10|5",
        "package_size_length": "30.0|30.0", # Made float like
        "package_size_width": "20.0|20.0",
        "package_size_height": "10.0|10.0",
        "package_weight": "0.5|0.5",
        "images": "http://example.com/img1.jpg|main_image:true|http://example.com/img2.jpg|main_image:false"
    }

@pytest.fixture
def sample_item_csv_model(sample_item_csv_row_dict) -> ItemCsvModel:
    return ItemCsvModel(**sample_item_csv_row_dict)

@pytest.fixture
def mock_parsed_attributes() -> List[Dict[str, Any]]:
    return [{'name': 'color', 'is_main': True}, {'name': 'size', 'is_main': False}]

@pytest.fixture
def mock_parsed_attr_values_by_type() -> List[List[Dict[str, Any]]]:
    return [
        [{'value': 'Black', 'is_default_sku_value': True}, {'value': 'White', 'is_default_sku_value': False}],
        [{'value': 'S'}, {'value': 'M'}]
    ]

@pytest.fixture
def mock_all_sku_variants() -> List[List[Dict[str, Any]]]:
    return [
        [{'attribute_name': 'color', 'value': 'Black', 'is_default_sku_value': True}, {'attribute_name': 'size', 'value': 'S'}],
        [{'attribute_name': 'color', 'value': 'Black', 'is_default_sku_value': True}, {'attribute_name': 'size', 'value': 'M'}],
        [{'attribute_name': 'color', 'value': 'White', 'is_default_sku_value': False}, {'attribute_name': 'size', 'value': 'S'}],
        [{'attribute_name': 'color', 'value': 'White', 'is_default_sku_value': False}, {'attribute_name': 'size', 'value': 'M'}],
    ]

# Mocks for data extractors, applied via autouse=True for convenience in this test file
@pytest.fixture(autouse=True)
def mock_all_data_extractors(mocker):
    # Using mocker.patch correctly
    mocker.patch('app.dataload.item_loader.get_price_for_combination', side_effect=lambda price_str, *args: float(price_str.split(':')[0].split('|')[0])) # Simplified mock
    mocker.patch('app.dataload.item_loader.get_quantity_for_combination', side_effect=lambda qty_str, *args: int(qty_str.split(':')[0].split('|')[0]))
    mocker.patch('app.dataload.item_loader.get_status_for_combination', return_value="ACTIVE") # Default to ACTIVE for tests
    mocker.patch('app.dataload.item_loader.get_order_limit_for_combination', return_value=5)
    mocker.patch('app.dataload.item_loader.get_package_size_length_for_combination', return_value=10.0)
    mocker.patch('app.dataload.item_loader.get_package_size_width_for_combination', return_value=10.0)
    mocker.patch('app.dataload.item_loader.get_package_size_height_for_combination', return_value=10.0)
    mocker.patch('app.dataload.item_loader.get_package_weight_for_combination', return_value=0.5)
    mocker.patch('app.dataload.item_loader.parse_product_level_images', return_value=[
        {'url': 'http://example.com/img1.jpg', 'main_image': True}
    ]) # Mock for image parsing utility

# --- Tests for load_item_record_to_db ---

@patch('app.dataload.item_loader.parse_attributes_string')
@patch('app.dataload.item_loader.parse_attribute_combination_string')
@patch('app.dataload.item_loader.generate_sku_variants')
@patch('app.dataload.item_loader._lookup_attribute_ids')
@patch('app.dataload.item_loader._lookup_attribute_value_ids')
@patch('app.dataload.item_loader.barcode_helper')
@patch('app.dataload.item_loader.now_epoch_ms', return_value=1234567890000)
def test_load_item_record_db_success_path(
    mock_now_epoch, mock_barcode_helper_module, # Renamed to avoid conflict if barcode_helper is also a var
    mock_lookup_val_ids, mock_lookup_attr_ids,
    mock_gen_variants, mock_parse_combo, mock_parse_attrs,
    mock_db_session, sample_item_csv_model,
    mock_parsed_attributes, mock_parsed_attr_values_by_type, mock_all_sku_variants,
    mock_all_data_extractors # Ensure this fixture is active
):
    mock_parse_attrs.return_value = mock_parsed_attributes
    mock_parse_combo.return_value = mock_parsed_attr_values_by_type
    mock_gen_variants.return_value = mock_all_sku_variants
    
    # Mock ProductOrm query to return an object with an 'id' attribute
    mock_product_orm_instance = MagicMock()
    mock_product_orm_instance.id = 1 # Product ID
    mock_db_session.query(ProductOrm.id).filter().one_or_none.return_value = mock_product_orm_instance
    
    mock_lookup_attr_ids.return_value = {'color': 10, 'size': 20}
    mock_lookup_val_ids.return_value = {
        ('color', 'Black'): 101, ('color', 'White'): 102,
        ('size', 'S'): 201, ('size', 'M'): 202
    }
    mock_barcode_helper_module.generate_barcode_image.return_value = b"barcode_bytes"
    mock_barcode_helper_module.encode_barcode_to_base64.return_value = "base64string"

    mock_id_counter = itertools.count(start=1001)
    
    # Capture added instances to inspect them
    added_instances_capture = []
    def assign_id_and_capture(instance):
        instance.id = next(mock_id_counter)
        added_instances_capture.append(instance)

    mock_db_session.add.side_effect = assign_id_and_capture
    
    created_main_sku_ids = load_item_record_to_db(mock_db_session, 1, sample_item_csv_model, 99)

    assert mock_db_session.query(ProductOrm.id).filter().one_or_none.call_count == 1
    mock_parse_attrs.assert_called_once_with(sample_item_csv_model.attributes)
    mock_parse_combo.assert_called_once_with(sample_item_csv_model.attribute_combination, mock_parsed_attributes)
    mock_gen_variants.assert_called_once_with(mock_parsed_attr_values_by_type, mock_parsed_attributes)
    
    assert len(created_main_sku_ids) == 4 

    # Expected adds: 4 MainSku, 4 Sku, 8 ProductVariant, 1 ProductImage
    assert mock_db_session.add.call_count == 17 
    assert mock_db_session.flush.call_count == (4 + 4 + 8) # After each MainSku, Sku, ProductVariant

    assert mock_barcode_helper_module.generate_barcode_image.call_count == 8 
    assert mock_barcode_helper_module.encode_barcode_to_base64.call_count == 8
    
    # Check first_main_sku_id_for_images (Black,S is the first default variant)
    # The MainSku for (Black,S) should be the first MainSkuOrm created.
    # Its ID will be 1001 if it's the first object to get an ID from mock_id_counter.
    # We need to ensure MainSkuOrm instances are created before SkuOrm or ProductVariantOrm in the loop for this to hold.
    # Based on current load_item_record_to_db, MainSkuOrm is created first in the loop.
    first_main_sku_instance = next(inst for inst in added_instances_capture if isinstance(inst, MainSkuOrm) and inst.is_default)
    
    image_orm_instance = next(inst for inst in added_instances_capture if isinstance(inst, ProductImageOrm))
    assert image_orm_instance.main_sku_id == first_main_sku_instance.id


def test_load_item_record_db_product_not_found(mock_db_session, sample_item_csv_model, mock_all_data_extractors):
    mock_db_session.query(ProductOrm.id).filter().one_or_none.return_value = None
    with pytest.raises(DataLoaderError, match="Product 'Test Product Alpha' not found"):
        load_item_record_to_db(mock_db_session, 1, sample_item_csv_model, 99)

@patch('app.dataload.item_loader.parse_attributes_string', side_effect=ItemParserError("Test parse error"))
def test_load_item_record_db_parser_error(mock_parse_attrs_func, mock_db_session, sample_item_csv_model, mock_all_data_extractors):
    # Mock ProductOrm query to return an object with an 'id' attribute
    mock_product_orm_instance = MagicMock()
    mock_product_orm_instance.id = 1 # Product ID
    mock_db_session.query(ProductOrm.id).filter().one_or_none.return_value = mock_product_orm_instance
    
    with pytest.raises(DataLoaderError, match="Error parsing item CSV structure.*Test parse error"):
        load_item_record_to_db(mock_db_session, 1, sample_item_csv_model, 99)

# --- Tests for load_items_to_db (Batch loader) ---

@patch('app.dataload.item_loader.load_item_record_to_db')
def test_load_items_to_db_success(mock_load_record_func, mock_db_session, sample_item_csv_row_dict, mock_all_data_extractors):
    mock_load_record_func.return_value = [101, 102] 
    item_records = [sample_item_csv_row_dict, sample_item_csv_row_dict] 
    
    summary = load_items_to_db(mock_db_session, 1, item_records, "test_session_01", 99)

    assert summary["csv_rows_processed"] == 2
    assert summary["csv_rows_with_errors"] == 0
    assert summary["total_main_skus_created_or_updated"] == 4 
    assert mock_load_record_func.call_count == 2
    assert mock_db_session.begin_nested.call_count == 2
    # Access the savepoint mock object returned by begin_nested() to check its methods
    mock_db_session.begin_nested().commit.assert_called_with() # Check commit on the savepoint object
    assert mock_db_session.begin_nested().commit.call_count == 2 
    assert mock_db_session.begin_nested().rollback.call_count == 0


@patch('app.dataload.item_loader.load_item_record_to_db')
def test_load_items_to_db_partial_failure(mock_load_record_func, mock_db_session, sample_item_csv_row_dict, mock_all_data_extractors):
    mock_load_record_func.side_effect = [[101, 102], DataLoaderError("Simulated error", ErrorType.VALIDATION)]
    item_records = [sample_item_csv_row_dict, sample_item_csv_row_dict]
    
    summary = load_items_to_db(mock_db_session, 1, item_records, "test_session_02", 99)

    assert summary["csv_rows_processed"] == 2
    assert summary["csv_rows_with_errors"] == 1
    assert summary["total_main_skus_created_or_updated"] == 2 
    assert mock_load_record_func.call_count == 2
    assert mock_db_session.begin_nested.call_count == 2
    mock_db_session.begin_nested().commit.assert_called_once() 
    mock_db_session.begin_nested().rollback.assert_called_once()


def test_load_items_to_db_pydantic_validation_error(mock_db_session, sample_item_csv_row_dict, mock_all_data_extractors):
    invalid_record = sample_item_csv_row_dict.copy()
    del invalid_record["product_name"] 
    item_records = [invalid_record]
    
    summary = load_items_to_db(mock_db_session, 1, item_records, "test_session_03", 99)
    
    assert summary["csv_rows_processed"] == 1
    assert summary["csv_rows_with_errors"] == 1
    assert summary["total_main_skus_created_or_updated"] == 0
    # begin_nested should not be called if pydantic validation fails for the row itself
    assert mock_db_session.begin_nested.call_count == 0
