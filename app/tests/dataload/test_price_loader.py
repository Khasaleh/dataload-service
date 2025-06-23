import pytest
import csv
from unittest.mock import MagicMock, patch, mock_open
from sqlalchemy.orm import Session
from app.dataload.price_loader import PriceLoader
from app.dataload.models.price_csv import PriceCsv, PriceTypeEnum
from app.db.models import Product, SKU, Price # Assuming these are the final model names

# Minimal Product and SKU ORM-like structures for mocking
class MockProduct:
    def __init__(self, id, business_details_id):
        self.id = id
        self.business_details_id = business_details_id

class MockSKU:
    def __init__(self, id, business_details_id):
        self.id = id
        self.business_details_id = business_details_id

@pytest.fixture
def mock_db_session():
    session = MagicMock(spec=Session)

    # Mock query mechanism
    # This is a simplified mock. A more complex one might use a dictionary to store "db" state.
    def mock_query_filter_first(model_class):
        # Based on how PriceLoader queries:
        # .query(Product).filter(Product.id == ...).first()
        # .query(SKU).filter(SKU.id == ...).first()
        # .query(Price).filter(...).first() for existing price checks (not explicitly in current PriceLoader._process_price_data yet)

        mock_query_obj = MagicMock()

        def filter_side_effect(*args, **kwargs):
            # This is where you'd check args (e.g., Product.id == price_data.product_id)
            # and return a mock product/sku or None

            # Example: if looking for Product with id "prod_exists"
            if model_class == Product:
                # Check filter arguments if Product.id == 'prod_exists'
                # This requires inspecting the SQLAlchemy filter expressions, which can be complex.
                # For simplicity, we'll assume if it's a Product query, and the test expects it to exist, we return one.
                # A more robust mock would parse the filter args.
                # Let's assume product_id 'prod_exists_123' should be found.
                # This part needs to be smarter based on the actual filter condition.
                # For now, let's make it return a specific product if a known ID is used in a test.

                # This is highly dependent on how the test sets up the filter condition.
                # A simple approach for this mock:
                if hasattr(args[0], 'left') and args[0].left.key == 'id':
                     # args[0].right.value would be the ID being queried
                    queried_id = str(args[0].right.value) # Make sure it's a string if IDs are strings
                    if queried_id == "prod_exists_123":
                        return MagicMock(first=lambda: MockProduct(id="prod_exists_123", business_details_id="biz_1"))
                    elif queried_id == "prod_for_price_update_1":
                         return MagicMock(first=lambda: MockProduct(id="prod_for_price_update_1", business_details_id="biz_1"))


            if model_class == SKU:
                if hasattr(args[0], 'left') and args[0].left.key == 'id':
                    queried_id = str(args[0].right.value)
                    if queried_id == "sku_exists_456":
                        return MagicMock(first=lambda: MockSKU(id="sku_exists_456", business_details_id="biz_1"))
                    elif queried_id == "sku_for_price_update_1":
                        return MagicMock(first=lambda: MockSKU(id="sku_for_price_update_1", business_details_id="biz_1"))

            # Mocking existing Price check for updates (conceptual, as loader needs to implement this first)
            if model_class == Price:
                # If PriceLoader._process_price_data queries for existing Price by product_id or sku_id
                # e.g. Price.product_id == 'prod_for_price_update_1'
                if hasattr(args[0], 'left') and args[0].left.key == 'product_id':
                    if str(args[0].right.value) == "prod_for_price_update_1":
                        # Return an existing Price object to be updated
                        existing_p = Price(id=101, product_id="prod_for_price_update_1", price=100.0, currency="USD", business_details_id="biz_1")
                        return MagicMock(first=lambda: existing_p)
                elif hasattr(args[0], 'left') and args[0].left.key == 'sku_id':
                     if str(args[0].right.value) == "sku_for_price_update_1":
                        existing_p_sku = Price(id=102, sku_id="sku_for_price_update_1", price=50.0, currency="USD", business_details_id="biz_1")
                        return MagicMock(first=lambda: existing_p_sku)


            return MagicMock(first=lambda: None) # Default to "not found"

        mock_query_obj.filter.side_effect = filter_side_effect
        return mock_query_obj

    session.query.side_effect = mock_query_filter_first
    return session

@pytest.fixture
def price_loader(mock_db_session):
    return PriceLoader(db_session=mock_db_session)

# --- Test Cases ---

def test_load_valid_product_price_new(price_loader, mock_db_session):
    csv_content = "price_type,product_id,sku_id,price,discount_price,cost_price,currency\n" \
                  "PRODUCT,prod_exists_123,,200.0,180.0,150.0,USD\n"

    with patch("builtins.open", mock_open(read_data=csv_content)):
        results = price_loader.load_prices_from_csv("dummy_path.csv")

    assert results["processed_count"] == 1
    assert results["error_count"] == 0
    assert not results["errors"]

    # Check that a Price object was added to the session
    # PriceLoader._process_price_data should call db_session.add() for new prices
    # This requires PriceLoader to actually create and add Price ORM objects.
    # For now, we check if Product.id was queried (implies product found)
    # and then we'd need to inspect what was 'add'ed.

    # Example check (if PriceLoader adds a Price ORM object):
    # mock_db_session.add.assert_called_once()
    # added_object = mock_db_session.add.call_args[0][0]
    # assert isinstance(added_object, Price)
    # assert added_object.product_id == "prod_exists_123"
    # assert added_object.price == 200.0
    # assert added_object.currency == "USD"

    # Since the current PriceLoader's _process_price_data is a placeholder for DB interaction,
    # we can't fully test the DB add yet. We verify it didn't error.
    # And that commit was called (if no errors)
    mock_db_session.commit.assert_called_once()


def test_load_valid_sku_price_new(price_loader, mock_db_session):
    csv_content = "price_type,product_id,sku_id,price,discount_price,cost_price,currency\n" \
                  "SKU,,sku_exists_456,120.0,,90.0,CAD\n"

    with patch("builtins.open", mock_open(read_data=csv_content)):
        results = price_loader.load_prices_from_csv("dummy_path.csv")

    assert results["processed_count"] == 1
    assert results["error_count"] == 0
    # Similar to above, detailed check of db_session.add would go here
    # mock_db_session.add.assert_called_once()
    # added_object = mock_db_session.add.call_args[0][0]
    # assert added_object.sku_id == "sku_exists_456"
    # assert added_object.price == 120.0
    # assert added_object.currency == "CAD"
    mock_db_session.commit.assert_called_once()


def test_load_price_product_not_found(price_loader, mock_db_session):
    csv_content = "price_type,product_id,sku_id,price\n" \
                  "PRODUCT,prod_not_exists,,100.0\n" # This product ID won't be found by the mock

    with patch("builtins.open", mock_open(read_data=csv_content)):
        results = price_loader.load_prices_from_csv("dummy_path.csv")

    assert results["processed_count"] == 1
    assert results["error_count"] == 1
    assert len(results["errors"]) == 1
    assert "Product with id prod_not_exists not found" in results["errors"][0]["errors"]
    mock_db_session.rollback.assert_called_once() # Rollback due to error


def test_load_price_sku_not_found(price_loader, mock_db_session):
    csv_content = "price_type,product_id,sku_id,price\n" \
                  "SKU,,sku_not_exists,50.0\n"

    with patch("builtins.open", mock_open(read_data=csv_content)):
        results = price_loader.load_prices_from_csv("dummy_path.csv")

    assert results["processed_count"] == 1
    assert results["error_count"] == 1
    assert "SKU with id sku_not_exists not found" in results["errors"][0]["errors"]
    mock_db_session.rollback.assert_called_once()

# --- Validation Error Tests (from PriceCsv model) ---

@pytest.mark.parametrize("csv_row, expected_error_part", [
    ("PRODUCT,,sku123,10.0", "product_id must be empty when price_type is SKU"), # Error: product_id should be empty for SKU
    ("PRODUCT,prod123,sku123,10.0", "sku_id must be empty when price_type is PRODUCT"), # Error: sku_id should be empty for PRODUCT
    ("SKU,prod123,,10.0", "sku_id is required when price_type is SKU"), # Error: sku_id required for SKU
    ("PRODUCT,, ,10.0", "product_id is required when price_type is PRODUCT"), # Error: product_id required for PRODUCT
    ("PRODUCT,prod_exists_123,,0", "price must be a positive number"), # Error: price not positive
    ("PRODUCT,prod_exists_123,,-10.0", "price must be a positive number"), # Error: price not positive
    ("PRODUCT,prod_exists_123,,10.0,12.0", "discount_price must be less than price"), # Error: discount > price
    ("PRODUCT,prod_exists_123,,10.0,10.0", "discount_price must be less than price"), # Error: discount == price
    ("PRODUCT,prod_exists_123,,10.0,,-5.0", "cost_price must be >= 0 if present"), # Error: cost_price negative
])
def test_csv_validation_errors(price_loader, mock_db_session, csv_row, expected_error_part):
    header = "price_type,product_id,sku_id,price,discount_price,cost_price,currency\n"
    # Pad the CSV row with commas if it's shorter than the header
    num_commas_header = header.count(',')
    num_commas_row = csv_row.count(',')
    padded_csv_row = csv_row + ',' * (num_commas_header - num_commas_row) + "\n"

    csv_content = header + padded_csv_row

    with patch("builtins.open", mock_open(read_data=csv_content)):
        results = price_loader.load_prices_from_csv("dummy_path.csv")

    assert results["processed_count"] == 1
    assert results["error_count"] == 1
    assert len(results["errors"]) == 1
    # Check if expected_error_part is in any of the error messages
    found_error = False
    for error_detail in results["errors"][0]["errors"]: # errors is a list of dicts from Pydantic
        if isinstance(error_detail, dict) and "msg" in error_detail:
            if expected_error_part in error_detail["msg"]:
                found_error = True
                break
        elif isinstance(error_detail, str): # Sometimes it's just a string for custom ValueErrors
             if expected_error_part in error_detail:
                found_error = True
                break
    assert found_error, f"Expected error part '{expected_error_part}' not found in {results['errors'][0]['errors']}"
    mock_db_session.rollback.assert_called_once()


def test_file_not_found(price_loader):
    with patch("builtins.open", mock_open()) as mock_file:
        mock_file.side_effect = FileNotFoundError("File not found for test")
        results = price_loader.load_prices_from_csv("non_existent_path.csv")

    assert results["error_count"] > 0 # Could be 0 processed if error is before loop
    assert any("File not found" in error.get("error", "") for error in results["errors"])


def test_empty_csv_file(price_loader):
    # CSV with only a header
    csv_content = "price_type,product_id,sku_id,price,discount_price,cost_price,currency\n"
    with patch("builtins.open", mock_open(read_data=csv_content)):
        results = price_loader.load_prices_from_csv("empty_data.csv")

    assert results["processed_count"] == 0
    assert results["error_count"] == 0
    assert not results["errors"]
    # Commit might be called if no rows and no errors, or rollback; depends on loader's exact logic for empty files.
    # Assuming commit if no errors and nothing to process.
    price_loader.db_session.commit.assert_called_once()


def test_csv_general_processing_error(price_loader):
    # Malformed CSV content that DictReader might struggle with or causes other errors
    csv_content = "price_type,product_id\nPRODUCT," # Missing fields, trailing comma

    # This test is a bit tricky as DictReader might still process this,
    # leading to validation errors instead of a general CSV processing error.
    # A true general error might be an unreadable file encoding or completely garbled content.
    # For this example, let's assume this leads to a PriceCsv validation error due to missing 'price'.

    with patch("builtins.open", mock_open(read_data=csv_content)):
        results = price_loader.load_prices_from_csv("malformed.csv")

    assert results["processed_count"] == 1
    assert results["error_count"] == 1
    assert "price" in results["errors"][0]["errors"][0]["loc"] # Pydantic error for missing 'price' field
    price_loader.db_session.rollback.assert_called_once()

# TODO: Add tests for updating existing prices once PriceLoader._process_price_data
# implements the logic to query for existing Price records and update them.
# Example:
# def test_load_product_price_update_existing(price_loader, mock_db_session):
#     # Setup mock_db_session.query(...).filter(...).first() to return an existing Price object
#     # for product_id 'prod_for_price_update_1'

#     csv_content = "price_type,product_id,sku_id,price,discount_price\n" \
#                   "PRODUCT,prod_for_price_update_1,,250.0,220.0\n"

#     with patch("builtins.open", mock_open(read_data=csv_content)):
#         results = price_loader.load_prices_from_csv("update_path.csv")

#     assert results["processed_count"] == 1
#     assert results["error_count"] == 0

#     # Assert that the existing Price object was modified, not a new one added
#     # mock_db_session.add.assert_not_called() # Or called if that's part of update logic
#     # Check properties of the "updated" Price object via mock_db_session or by inspecting
#     # the object that would have been returned by the query.
#     # This depends heavily on the PriceLoader's update implementation.
#     mock_db_session.commit.assert_called_once()

# Note: The current PriceLoader's _process_price_data method is a placeholder.
# These tests are written assuming it will be fleshed out to:
# 1. Query for Product/SKU by ID.
# 2. If creating/updating a Price DB object:
#    - Query for an existing Price for that Product/SKU.
#    - If exists, update its fields.
#    - If not, create a new Price object (e.g., `Price(...)`) and call `db_session.add()`.
# The mock_db_session fixture needs to be adjusted to support these queries for Price objects
# once the loader implements that logic.
# The tests for "new" prices implicitly test the "creation" path.
# Tests for "updates" would require the mock to return an existing Price object for certain IDs.

# Further improvements for mock_db_session:
# - A more stateful mock that can "remember" what's been added/committed/rolled back.
# - Better parsing of filter arguments in the query mock to be more specific.
# - Using a real in-memory SQLite DB for some integration-style unit tests if feasible.
# For now, the focus is on the PriceLoader's CSV reading and validation logic.
# The DB interaction part of _process_price_data is critical for full testing.
# The current mock_db_session is a starting point.
# The current PriceLoader code does not yet query for existing Price records to update them.
# It only queries for Product/SKU existence. So, `test_load_product_price_update_existing`
# would fail or not test the update path correctly until the loader is updated.
# The `mock_query_filter_first` in `mock_db_session` has a conceptual part for Price queries.
# It needs to be enabled/refined when PriceLoader has update logic.

# The current PriceLoader's `_process_price_data` does:
# 1. Find target_entity (Product/SKU)
# 2. If found, logs "Successfully validated price data"
# It does NOT yet:
#   - Query for existing Price record for this product/SKU.
#   - Create a new Price record and `db_session.add(new_price_db_object)`.
#   - Update an existing Price record.
# The tests `test_load_valid_product_price_new` and `test_load_valid_sku_price_new`
# will pass the validation stage and call `db_session.commit()` because no *processing* error occurs.
# However, they don't yet test that a `Price` ORM object is correctly constructed and added/updated.
# This functionality needs to be added to `PriceLoader._process_price_data` first.
# The tests are structured to be ready for when that logic is in place.
# For now, they confirm that valid CSV rows for existing products/SKUs don't cause loader-level errors.

# After PriceLoader._process_price_data is updated to actually create/update Price ORM objects:
# - Uncomment and refine the mock_db_session.add.assert_called_once() checks.
# - Implement and uncomment test_load_product_price_update_existing and similar for SKUs.
# - The mock_db_session's query for Price will become active.
# - Ensure the mock_db_session correctly simulates the state of the "database" for these tests.
#   (e.g., what happens after an add, commit, rollback).
# The current tests primarily validate the CSV parsing, Pydantic model validation,
# and Product/SKU existence checks within the loader.
```
