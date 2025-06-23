import pytest
from unittest.mock import MagicMock, patch, mock_open
from sqlalchemy.orm import Session

from app.dataload.meta_tags_loader import load_meta_tags_from_csv #, DataloadSummary, DataloadErrorDetail (not needed for import here)
# from app.dataload.models.meta_tags_csv import MetaTagCsvRow, MetaTypeEnum (not directly used in test functions)
from app.db.models import ProductOrm, CategoryOrm

CSV_HEADERS = "meta_type,target_identifier,business_details_id,meta_title,meta_description,meta_keywords\n"

class TestLoadMetaTagsFromCsv:

    @pytest.fixture
    def mock_db_session(self):
        session = MagicMock(spec=Session)
        # Configure query chain to return a mock that can have first() called.
        mock_query = MagicMock()
        mock_query.filter.return_value.first.return_value = None # Default: target not found
        session.query.return_value = mock_query
        return session

    def test_successful_product_update(self, mock_db_session):
        mock_product = ProductOrm(id=1, name="Prod1", business_details_id=100)
        # Adjust mock to simulate product found
        mock_db_session.query(ProductOrm).filter.return_value.first.return_value = mock_product

        csv_data = CSV_HEADERS + "PRODUCT,Prod1,100,New Title,New Desc,new,keys"

        with patch("builtins.open", mock_open(read_data=csv_data)) as mock_file_open:
            summary = load_meta_tags_from_csv(mock_db_session, "dummy_path.csv")

        mock_file_open.assert_called_once_with("dummy_path.csv", mode='r', encoding='utf-8-sig')
        assert summary.total_rows_processed == 1
        assert summary.successful_updates == 1
        assert summary.validation_errors == 0
        assert summary.target_not_found_errors == 0
        assert summary.database_errors == 0
        assert not summary.error_details

        assert mock_product.seo_title == "New Title"
        assert mock_product.seo_description == "New Desc"
        assert mock_product.keywords == "new,keys"
        mock_db_session.commit.assert_called_once()

    def test_successful_category_update(self, mock_db_session):
        mock_category = CategoryOrm(id=1, name="Cat1")
        mock_db_session.query(CategoryOrm).filter.return_value.first.return_value = mock_category

        csv_data = CSV_HEADERS + "CATEGORY,Cat1,,New Cat Title,New Cat Desc,newcat,keys"

        with patch("builtins.open", mock_open(read_data=csv_data)):
            summary = load_meta_tags_from_csv(mock_db_session, "dummy_path.csv")

        assert summary.total_rows_processed == 1
        assert summary.successful_updates == 1
        assert not summary.error_details
        assert mock_category.seo_title == "New Cat Title"
        assert mock_category.seo_description == "New Cat Desc"
        assert mock_category.seo_keywords == "newcat,keys"
        mock_db_session.commit.assert_called_once()

    def test_validation_error_for_row(self, mock_db_session):
        csv_data = CSV_HEADERS + "PRODUCT,Prod1,,Invalid Product Row,Desc,Key"

        with patch("builtins.open", mock_open(read_data=csv_data)):
            summary = load_meta_tags_from_csv(mock_db_session, "dummy_path.csv")

        assert summary.total_rows_processed == 1
        assert summary.successful_updates == 0
        assert summary.validation_errors == 1
        assert len(summary.error_details) == 1
        assert summary.error_details[0].row_number == 2
        assert summary.error_details[0].error_type == "Validation"
        assert "business_details_id is required" in summary.error_details[0].error_message
        mock_db_session.commit.assert_not_called()
        # Rollback is called after validation error to ensure clean state for next row processing
        mock_db_session.rollback.assert_called_once()


    def test_product_not_found(self, mock_db_session):
        # mock_db_session.query(...).first() returns None by default from fixture for ProductOrm
        csv_data = CSV_HEADERS + "PRODUCT,UnknownProd,100,Title,Desc,keys"

        with patch("builtins.open", mock_open(read_data=csv_data)):
            summary = load_meta_tags_from_csv(mock_db_session, "dummy_path.csv")

        assert summary.total_rows_processed == 1
        assert summary.target_not_found_errors == 1
        assert len(summary.error_details) == 1
        assert summary.error_details[0].error_type == "NotFound"
        assert "PRODUCT with name 'UnknownProd' and business_details_id '100' not found" in summary.error_details[0].error_message
        mock_db_session.commit.assert_not_called()
        mock_db_session.rollback.assert_called_once()


    def test_category_not_found(self, mock_db_session):
        # mock_db_session.query(...).first() returns None by default from fixture for CategoryOrm
        csv_data = CSV_HEADERS + "CATEGORY,UnknownCat,,Title,Desc,keys"
        with patch("builtins.open", mock_open(read_data=csv_data)):
            summary = load_meta_tags_from_csv(mock_db_session, "dummy_path.csv")

        assert summary.total_rows_processed == 1
        assert summary.target_not_found_errors == 1
        assert len(summary.error_details) == 1
        assert summary.error_details[0].error_type == "NotFound"
        assert "CATEGORY with name 'UnknownCat' not found" in summary.error_details[0].error_message
        mock_db_session.commit.assert_not_called()
        mock_db_session.rollback.assert_called_once()


    def test_database_error_on_update(self, mock_db_session):
        mock_product = ProductOrm(id=1, name="DBErrorProd", business_details_id=100)
        mock_db_session.query(ProductOrm).filter.return_value.first.return_value = mock_product
        mock_db_session.commit.side_effect = Exception("Simulated DB commit error")

        csv_data = CSV_HEADERS + "PRODUCT,DBErrorProd,100,Title,Desc,keys"
        with patch("builtins.open", mock_open(read_data=csv_data)):
            summary = load_meta_tags_from_csv(mock_db_session, "dummy_path.csv")

        assert summary.total_rows_processed == 1
        assert summary.successful_updates == 0
        assert summary.database_errors == 1
        assert len(summary.error_details) == 1
        assert summary.error_details[0].error_type == "Database"
        assert "Simulated DB commit error" in summary.error_details[0].error_message
        mock_db_session.rollback.assert_called_once()

    def test_file_not_found_error(self, mock_db_session):
        with patch("builtins.open", mock_open()) as mock_file_open:
            mock_file_open.side_effect = FileNotFoundError("File not really there")
            summary = load_meta_tags_from_csv(mock_db_session, "non_existent_path.csv")

        assert summary.total_rows_processed == 0
        assert summary.successful_updates == 0
        assert len(summary.error_details) == 1
        assert summary.error_details[0].error_type == "FileAccess"
        assert "CSV file not found" in summary.error_details[0].error_message

    def test_empty_csv_file(self, mock_db_session):
        csv_data = CSV_HEADERS
        with patch("builtins.open", mock_open(read_data=csv_data)):
            summary = load_meta_tags_from_csv(mock_db_session, "dummy_path.csv")

        assert summary.total_rows_processed == 0
        assert summary.successful_updates == 0
        assert not summary.error_details

    def test_csv_with_mixed_rows(self, mock_db_session):
        mock_product1 = ProductOrm(id=1, name="Prod1", business_details_id=100)
        mock_category1 = CategoryOrm(id=1, name="Cat1")

        # query.filter().first() results for each call
        # 1st product call (Prod1), 2nd category call (Cat1), 3rd product call (ProdX - validation fail, no DB), 4th product call (Prod2 - not found)
        mock_db_session.query(ProductOrm).filter.return_value.first.side_effect = [mock_product1, None]
        mock_db_session.query(CategoryOrm).filter.return_value.first.return_value = mock_category1

        csv_data = (
            CSV_HEADERS +
            "PRODUCT,Prod1,100,P1 Title,P1 Desc,p1k\n" +
            "CATEGORY,Cat1,,C1 Title,C1 Desc,c1k\n" +
            "PRODUCT,ProdX,,Invalid Product,Desc,Key\n" +
            "PRODUCT,Prod2,101,P2 Title,P2 Desc,p2k\n"
        )
        with patch("builtins.open", mock_open(read_data=csv_data)):
            summary = load_meta_tags_from_csv(mock_db_session, "dummy_path.csv")

        assert summary.total_rows_processed == 4
        assert summary.successful_updates == 2
        assert summary.validation_errors == 1
        assert summary.target_not_found_errors == 1
        assert summary.database_errors == 0
        assert len(summary.error_details) == 2

        error_messages = [e.error_message for e in summary.error_details]
        assert any("business_details_id is required" in msg for msg in error_messages)
        assert any("PRODUCT with name 'Prod2' and business_details_id '101' not found" in msg for msg in error_messages)

        assert mock_db_session.commit.call_count == 2
        assert mock_db_session.rollback.call_count == 2 # Once for validation error, once for not found

    def test_no_update_if_data_is_same(self, mock_db_session):
        mock_product = ProductOrm(
            id=1, name="Prod1", business_details_id=100,
            seo_title="Existing Title", seo_description="Existing Desc", keywords="existing,keys"
        )
        mock_db_session.query(ProductOrm).filter.return_value.first.return_value = mock_product

        csv_data = CSV_HEADERS + "PRODUCT,Prod1,100,Existing Title,Existing Desc,existing,keys"

        with patch("builtins.open", mock_open(read_data=csv_data)):
            summary = load_meta_tags_from_csv(mock_db_session, "dummy_path.csv")

        assert summary.total_rows_processed == 1
        assert summary.successful_updates == 0
        assert not summary.error_details
        mock_db_session.commit.assert_not_called()

    def test_partial_update_product(self, mock_db_session):
        mock_product = ProductOrm(
            id=1, name="Prod1", business_details_id=100,
            seo_title="Old Title", seo_description="Old Desc", keywords="old,keys"
        )
        mock_db_session.query(ProductOrm).filter.return_value.first.return_value = mock_product

        csv_data = CSV_HEADERS + "PRODUCT,Prod1,100,New Title,," # Desc and keywords are empty in CSV

        with patch("builtins.open", mock_open(read_data=csv_data)):
            summary = load_meta_tags_from_csv(mock_db_session, "dummy_path.csv")

        assert summary.total_rows_processed == 1
        assert summary.successful_updates == 1
        assert mock_product.seo_title == "New Title"
        assert mock_product.seo_description == "Old Desc" # Should not change if CSV field is empty and model field had value
        assert mock_product.keywords == "old,keys"     # Should not change
        mock_db_session.commit.assert_called_once()

    def test_key_cleaning_from_csv_header(self, mock_db_session):
        mock_product = ProductOrm(id=1, name="Prod1", business_details_id=100)
        mock_db_session.query(ProductOrm).filter.return_value.first.return_value = mock_product

        # CSV with spaces in headers
        csv_data_spaced_headers = "Meta Type,Target Identifier,Business Details Id,Meta Title,Meta Description,Meta Keywords\n" + \
                                  "PRODUCT,Prod1,100,Spaced Title,Spaced Desc,spaced,keys"

        with patch("builtins.open", mock_open(read_data=csv_data_spaced_headers)) as mock_file_open:
            summary = load_meta_tags_from_csv(mock_db_session, "dummy_path.csv")

        assert summary.total_rows_processed == 1
        assert summary.successful_updates == 1
        assert mock_product.seo_title == "Spaced Title"
        mock_db_session.commit.assert_called_once()
