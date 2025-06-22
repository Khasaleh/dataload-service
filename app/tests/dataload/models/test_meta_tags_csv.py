import pytest
from pydantic import ValidationError
from app.dataload.models.meta_tags_csv import MetaTagCsvRow, MetaTypeEnum

# Valid data samples
VALID_PRODUCT_ROW_FULL = {
    "meta_type": "PRODUCT",
    "target_identifier": "Nike Shoes",
    "business_details_id": "101", # String, will be converted to int
    "meta_title": "Buy Nike Shoes Online",
    "meta_description": "Best running shoes for performance.",
    "meta_keywords": "running, shoes, nike"
}

VALID_PRODUCT_ROW_MINIMAL = {
    "meta_type": "PRODUCT",
    "target_identifier": "Old Blender",
    "business_details_id": 102 # Int directly
}

VALID_CATEGORY_ROW_FULL = {
    "meta_type": "CATEGORY",
    "target_identifier": "Home Appliances",
    "business_details_id": "", # Should become None
    "meta_title": "Best Home Appliances",
    "meta_description": "Find deals on top appliances",
    "meta_keywords": "appliances, kitchen, home"
}

VALID_CATEGORY_ROW_MINIMAL = {
    "meta_type": "CATEGORY",
    "target_identifier": "Footwear"
    # Other fields will be None
}

class TestMetaTagCsvRow:
    def test_valid_product_full(self):
        row = MetaTagCsvRow(**VALID_PRODUCT_ROW_FULL)
        assert row.meta_type == MetaTypeEnum.PRODUCT
        assert row.target_identifier == "Nike Shoes"
        assert row.business_details_id == 101
        assert row.meta_title == "Buy Nike Shoes Online"
        assert row.meta_description == "Best running shoes for performance."
        assert row.meta_keywords == "running, shoes, nike"

    def test_valid_product_minimal(self):
        row = MetaTagCsvRow(**VALID_PRODUCT_ROW_MINIMAL)
        assert row.meta_type == MetaTypeEnum.PRODUCT
        assert row.target_identifier == "Old Blender"
        assert row.business_details_id == 102
        assert row.meta_title is None
        assert row.meta_description is None
        assert row.meta_keywords is None

    def test_valid_category_full(self):
        data = VALID_CATEGORY_ROW_FULL.copy()
        row = MetaTagCsvRow(**data)
        assert row.meta_type == MetaTypeEnum.CATEGORY
        assert row.target_identifier == "Home Appliances"
        assert row.business_details_id is None # Empty string converted by preprocess_data
        assert row.meta_title == "Best Home Appliances"
        assert row.meta_description == "Find deals on top appliances"
        assert row.meta_keywords == "appliances, kitchen, home"

    def test_valid_category_minimal(self):
        row = MetaTagCsvRow(**VALID_CATEGORY_ROW_MINIMAL)
        assert row.meta_type == MetaTypeEnum.CATEGORY
        assert row.target_identifier == "Footwear"
        assert row.business_details_id is None
        assert row.meta_title is None
        assert row.meta_description is None
        assert row.meta_keywords is None

    def test_invalid_meta_type(self):
        data = VALID_PRODUCT_ROW_FULL.copy()
        data["meta_type"] = "INVALID_TYPE"
        with pytest.raises(ValidationError) as excinfo:
            MetaTagCsvRow(**data)
        assert "meta_type" in str(excinfo.value) # Pydantic will show error about enum mismatch

    def test_product_missing_business_id(self):
        data = VALID_PRODUCT_ROW_FULL.copy()
        # Test completely missing business_details_id
        minimal_product_data = {"meta_type": "PRODUCT", "target_identifier": "ProdX"}
        with pytest.raises(ValidationError, match="business_details_id is required for PRODUCT meta_type"):
            MetaTagCsvRow(**minimal_product_data)

        # Test business_details_id as empty string (should be caught by validator after preprocess)
        data_empty_biz_id = {"meta_type": "PRODUCT", "target_identifier": "ProdY", "business_details_id": ""}
        with pytest.raises(ValidationError, match="business_details_id is required for PRODUCT meta_type"):
            MetaTagCsvRow(**data_empty_biz_id)

        # Test business_details_id as non-integer string
        data_invalid_biz_id = {"meta_type": "PRODUCT", "target_identifier": "ProdZ", "business_details_id": "not_an_int"}
        with pytest.raises(ValidationError) as excinfo: # Pydantic's own type error for int
            MetaTagCsvRow(**data_invalid_biz_id)
        assert "business_details_id" in str(excinfo.value)
        assert "Input should be a valid integer" in str(excinfo.value)


    def test_missing_target_identifier(self):
        data = VALID_PRODUCT_ROW_FULL.copy()
        data["target_identifier"] = "" # Empty string after strip
        with pytest.raises(ValidationError) as excinfo:
            MetaTagCsvRow(**data)
        assert "target_identifier" in str(excinfo.value)
        assert "String should have at least 1 character" in str(excinfo.value)

    def test_meta_title_too_long(self):
        data = VALID_PRODUCT_ROW_FULL.copy()
        data["meta_title"] = "a" * 257
        with pytest.raises(ValidationError) as excinfo:
            MetaTagCsvRow(**data)
        assert "meta_title" in str(excinfo.value)
        assert "ensure this value has at most 256 characters" in str(excinfo.value) # Pydantic v1 message for constr

    # Product description/keywords length tests
    def test_product_meta_description_too_long(self):
        data = VALID_PRODUCT_ROW_FULL.copy()
        data["meta_description"] = "a" * 513
        with pytest.raises(ValidationError, match="meta_description for PRODUCT .* cannot exceed 512 characters"):
            MetaTagCsvRow(**data)

    def test_product_meta_keywords_too_long(self):
        data = VALID_PRODUCT_ROW_FULL.copy()
        data["meta_keywords"] = "a" * 513
        with pytest.raises(ValidationError, match="meta_keywords for PRODUCT .* cannot exceed 512 characters"):
            MetaTagCsvRow(**data)

    # Category description/keywords length tests
    def test_category_meta_description_too_long(self):
        data = VALID_CATEGORY_ROW_FULL.copy()
        data["meta_description"] = "a" * 256
        with pytest.raises(ValidationError, match="meta_description for CATEGORY .* cannot exceed 255 characters"):
            MetaTagCsvRow(**data)

    def test_category_meta_keywords_too_long(self):
        data = VALID_CATEGORY_ROW_FULL.copy()
        data["meta_keywords"] = "a" * 256
        with pytest.raises(ValidationError, match="meta_keywords for CATEGORY .* cannot exceed 255 characters"):
            MetaTagCsvRow(**data)

    def test_whitespace_stripping_and_conversion_in_preprocess(self):
        data = {
            "meta_type": " PRODUCT ",
            "target_identifier": "  Test Product  ",
            "business_details_id": " 123 ", # String with spaces
            "meta_title": "  My Title  ",
            "meta_description": "  Desc.  ",
            "meta_keywords": "  key1, key2  "
        }
        # preprocess_data in MetaTagCsvRow handles stripping for all string fields
        # and conversion of business_details_id to int.
        row = MetaTagCsvRow(**data)
        assert row.meta_type == MetaTypeEnum.PRODUCT # Enum conversion handles stripping
        assert row.target_identifier == "Test Product" # Stripped by preprocess
        assert row.business_details_id == 123 # Stripped then int converted
        assert row.meta_title == "My Title" # Stripped by preprocess
        assert row.meta_description == "Desc." # Stripped by preprocess
        assert row.meta_keywords == "key1, key2" # Stripped by preprocess

    def test_empty_optional_fields_become_none_in_preprocess(self):
        data = {
            "meta_type": "PRODUCT",
            "target_identifier": "Test Product",
            "business_details_id": "123",
            "meta_title": " ", # whitespace only string
            "meta_description": "", # empty string
            "meta_keywords": None # already None
        }
        # preprocess_data converts "" or " " (after strip) to None for these fields
        row = MetaTagCsvRow(**data)
        assert row.meta_title is None
        assert row.meta_description is None
        assert row.meta_keywords is None

    def test_extra_fields_forbidden(self):
        data = VALID_PRODUCT_ROW_FULL.copy()
        data["extra_field"] = "some_value"
        with pytest.raises(ValidationError) as excinfo:
            MetaTagCsvRow(**data)
        assert "extra_field" in str(excinfo.value)
        # Pydantic v1: "extra fields not permitted"
        # Pydantic v2: "Extra inputs are not permitted"
        assert "Extra inputs are not permitted" in str(excinfo.value) or "extra fields not permitted" in str(excinfo.value)

    def test_integer_business_id_direct(self):
        data = VALID_PRODUCT_ROW_FULL.copy()
        data["business_details_id"] = 101 # Pass as int
        row = MetaTagCsvRow(**data)
        assert row.business_details_id == 101

    def test_product_business_id_none_for_category(self):
        # business_details_id is optional for category and should be None if not provided or empty
        data = {"meta_type": "CATEGORY", "target_identifier": "CatName", "business_details_id": None}
        row = MetaTagCsvRow(**data)
        assert row.business_details_id is None

        data_empty = {"meta_type": "CATEGORY", "target_identifier": "CatName", "business_details_id": ""}
        row_empty = MetaTagCsvRow(**data_empty)
        assert row_empty.business_details_id is None
