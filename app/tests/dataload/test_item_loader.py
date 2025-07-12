import pytest
from unittest.mock import MagicMock, patch, call
from sqlalchemy.orm import Session
from app.dataload.models.item_csv import ItemCsvModel
from app.dataload.item_loader import load_item_record_to_db, load_items_to_db
from app.db.models import ProductOrm, AttributeOrm, AttributeValueOrm, SkuOrm, MainSkuOrm, ProductVariantOrm, ProductImageOrm
from app.exceptions import DataLoaderError

@pytest.fixture
def mock_db_session():
    """Provides a MagicMock for the database session."""
    return MagicMock(spec=Session)

@pytest.fixture
def mock_product():
    """Provides a mock product ORM object."""
    product = ProductOrm(id=1, name="Test Product", business_details_id=1)
    return product

@pytest.fixture
def mock_attributes(mock_db_session, mock_product):
    """Mocks the attribute and attribute value lookups."""
    color_attr = AttributeOrm(id=1, name="color", business_details_id=mock_product.business_details_id)
    size_attr = AttributeOrm(id=2, name="size", business_details_id=mock_product.business_details_id)
    
    black_val = AttributeValueOrm(id=10, attribute_id=1, name="Black")
    white_val = AttributeValueOrm(id=11, attribute_id=1, name="White")
    small_val = AttributeValueOrm(id=20, attribute_id=2, name="S")
    medium_val = AttributeValueOrm(id=21, attribute_id=2, name="M")

    def query_side_effect(*args, **kwargs):
        if args[0] == ProductOrm:
            return MagicMock(filter=MagicMock(return_value=MagicMock(one=lambda: mock_product)))
        if args[0] == AttributeOrm:
            mock_query = MagicMock()
            def filter_side_effect(*fargs, **fkwargs):
                if 'color' in str(fargs[0]).lower():
                    return MagicMock(one=lambda: color_attr)
                if 'size' in str(fargs[0]).lower():
                    return MagicMock(one=lambda: size_attr)
                return MagicMock(one=MagicMock(side_effect=NoResultFound))
            mock_query.filter.side_effect = filter_side_effect
            return mock_query
        if args[0] == AttributeValueOrm:
            mock_query = MagicMock()
            def filter_side_effect(*fargs, **fkwargs):
                if 'black' in str(fargs[1]).lower():
                    return MagicMock(one=lambda: black_val)
                if 'white' in str(fargs[1]).lower():
                    return MagicMock(one=lambda: white_val)
                if 's' in str(fargs[1]).lower():
                    return MagicMock(one=lambda: small_val)
                if 'm' in str(fargs[1]).lower():
                    return MagicMock(one=lambda: medium_val)
                return MagicMock(one=MagicMock(side_effect=NoResultFound))
            mock_query.filter.side_effect = filter_side_effect
            return mock_query
        return MagicMock()

    mock_db_session.query.side_effect = query_side_effect
    return mock_db_session


def test_load_item_record_to_db_success(mock_attributes):
    """Tests successful loading of a single item record with variants."""
    item_csv = ItemCsvModel(
        product_name="Test Product",
        attributes="color|main_attribute:true|size|main_attribute:false",
        attribute_combination="{Black|main_sku:true:White|main_sku:false}|{S:M}",
        price="10:12|11:13",
        quantity="100:110|105:115",
        status="ACTIVE|ACTIVE",
        order_limit="10|10",
        package_size_length="5|5",
        package_size_width="5|5",
        package_size_height="2|2",
        package_weight="0.5|0.5",
        images="{https://example.com/image1.jpg|main_image:true}"
    )
    
    with patch('app.utils.barcode_helper.generate_barcode_image', return_value=b'barcode_bytes'), \
         patch('app.utils.barcode_helper.encode_barcode_to_base64', return_value='base64_barcode'):

        result = load_item_record_to_db(mock_attributes, 1, item_csv, 1)

        assert len(result) > 0
        assert mock_attributes.add.call_count > 0 # Checks if db.add was called for new SKUs etc.

def test_load_item_record_to_db_product_not_found(mock_db_session):
    """Tests failure when the product is not found in the database."""
    mock_db_session.query.return_value.filter.return_value.one.side_effect = NoResultFound
    item_csv = ItemCsvModel(
        product_name="Unknown Product",
        attributes="color|main_attribute:true",
        attribute_combination="{Red|main_sku:true}",
        price="10",
        quantity="100",
        status="ACTIVE"
    )
    with pytest.raises(DataLoaderError) as excinfo:
        load_item_record_to_db(mock_db_session, 1, item_csv, 1)
    assert "not found" in str(excinfo.value)


def test_load_items_to_db_batch_processing(mock_attributes):
    """Tests the batch loading function with multiple records."""
    item_records = [
        {
            "product_name": "Test Product",
            "attributes": "color|main_attribute:true|size|main_attribute:false",
            "attribute_combination": "{Black|main_sku:true:White|main_sku:false}|{S:M}",
            "price": "10:12|11:13",
            "quantity": "100:110|105:115",
            "status": "ACTIVE|ACTIVE",
            "images": "{https://example.com/image1.jpg|main_image:true}"
        },
        {
            "product_name": "Another Product", # This will fail lookup
            "attributes": "color|main_attribute:true",
            "attribute_combination": "{Blue|main_sku:true}",
            "price": "20",
            "quantity": "50",
            "status": "ACTIVE"
        }
    ]

    # Mock product lookup to fail for the second product
    def product_query_side_effect(*args, **kwargs):
        if "Test Product" in str(kwargs.get('name')):
            return MagicMock(one=lambda: ProductOrm(id=1, name="Test Product", business_details_id=1))
        return MagicMock(one=MagicMock(side_effect=NoResultFound))

    mock_attributes.query.return_value.filter.side_effect = product_query_side_effect

    with patch('app.dataload.item_loader.load_item_record_to_db') as mock_load_record:
        # First call succeeds, second raises DataLoaderError
        mock_load_record.side_effect = [[1, 2, 3, 4], DataLoaderError("Product not found")]

        summary = load_items_to_db(mock_attributes, 1, item_records, "session123", 1)

        assert summary["csv_rows_processed"] == 2
        assert summary["csv_rows_with_errors"] == 1
        assert summary["total_main_skus_created_or_updated"] == 4
        assert mock_load_record.call_count == 2
