import pytest
from app.dataload.parsers.item_parser import (
    parse_attributes_string,
    parse_attribute_combination_string,
    generate_sku_variants,
    ItemParserError
)

# --- Tests for parse_attributes_string ---

def test_parse_attributes_valid():
    attr_str = "color|main_attribute:true|size|main_attribute:false|material|main_attribute:false"
    expected = [
        {'name': 'color', 'is_main': True},
        {'name': 'size', 'is_main': False},
        {'name': 'material', 'is_main': False}
    ]
    assert parse_attributes_string(attr_str) == expected

def test_parse_attributes_single_attribute_main_true():
    attr_str = "finish|main_attribute:true"
    expected = [{'name': 'finish', 'is_main': True}]
    assert parse_attributes_string(attr_str) == expected

def test_parse_attributes_single_attribute_main_false_raises_error():
    attr_str = "finish|main_attribute:false"
    with pytest.raises(ItemParserError, match="No attribute was marked as 'main_attribute:true'"):
        parse_attributes_string(attr_str)

def test_parse_attributes_empty_string():
    with pytest.raises(ItemParserError, match="Attributes string cannot be empty."):
        parse_attributes_string("")

def test_parse_attributes_malformed_odd_parts():
    attr_str = "color|main_attribute:true|size"
    with pytest.raises(ItemParserError, match="Expected pairs of attribute name and main_attribute flag"):
        parse_attributes_string(attr_str)

def test_parse_attributes_malformed_invalid_flag():
    attr_str = "color|main_attribute:on|size|main_attribute:false"
    with pytest.raises(ItemParserError, match="Invalid main_attribute flag 'main_attribute:on'"):
        parse_attributes_string(attr_str)

def test_parse_attributes_multiple_main_true():
    attr_str = "color|main_attribute:true|size|main_attribute:true"
    with pytest.raises(ItemParserError, match="Multiple main attributes defined"):
        parse_attributes_string(attr_str)

def test_parse_attributes_no_main_true_multiple_attributes():
    attr_str = "color|main_attribute:false|size|main_attribute:false"
    with pytest.raises(ItemParserError, match="No attribute was marked as 'main_attribute:true'"):
        parse_attributes_string(attr_str)
        
def test_parse_attributes_empty_name_start():
    attr_str = "|main_attribute:true|size|main_attribute:false"
    with pytest.raises(ItemParserError, match="First attribute name cannot be empty"):
        parse_attributes_string(attr_str)

def test_parse_attributes_empty_name_middle():
    attr_str = "color|main_attribute:true||main_attribute:false" # Empty name for second attribute
    with pytest.raises(ItemParserError, match="Attribute name cannot be empty"):
        parse_attributes_string(attr_str)

# --- Tests for parse_attribute_combination_string ---

@pytest.fixture
def sample_parsed_attributes_fixture():
    # Used by multiple tests for parse_attribute_combination_string and generate_sku_variants
    return [
        {'name': 'color', 'is_main': True},
        {'name': 'size', 'is_main': False}
    ]

def test_parse_combinations_valid(sample_parsed_attributes_fixture):
    combo_str = "{Black|main_sku:true:White|main_sku:false}|{S:M:L}"
    expected = [
        [{'value': 'Black', 'is_default_sku_value': True}, {'value': 'White', 'is_default_sku_value': False}],
        [{'value': 'S'}, {'value': 'M'}, {'value': 'L'}]
    ]
    assert parse_attribute_combination_string(combo_str, sample_parsed_attributes_fixture) == expected

def test_parse_combinations_single_main_attribute(sample_parsed_attributes_fixture):
    parsed_attrs_single_main = [sample_parsed_attributes_fixture[0]] # Only 'color' which is main
    combo_str = "{Red|main_sku:true:Blue|main_sku:false}"
    expected = [
        [{'value': 'Red', 'is_default_sku_value': True}, {'value': 'Blue', 'is_default_sku_value': False}]
    ]
    assert parse_attribute_combination_string(combo_str, parsed_attrs_single_main) == expected
    
def test_parse_combinations_single_non_main_attribute_type_scenario():
    # This tests parsing for a non-main attribute group if it were the only group.
    # parse_attributes_string would raise error if it's the only attribute and not main:true.
    # So we manually construct parsed_attributes for this specific test of combination parsing.
    parsed_attrs_single_non_main = [{'name': 'finish', 'is_main': False}]
    combo_str = "{Matte:Glossy}"
    expected = [
        [{'value': 'Matte'}, {'value': 'Glossy'}]
    ]
    assert parse_attribute_combination_string(combo_str, parsed_attrs_single_non_main) == expected

def test_parse_combinations_empty_string(sample_parsed_attributes_fixture):
    with pytest.raises(ItemParserError, match="Attribute combination string cannot be empty"):
        parse_attribute_combination_string("", sample_parsed_attributes_fixture)

def test_parse_combinations_no_parsed_attributes():
    with pytest.raises(ItemParserError, match="Parsed attributes list cannot be empty"):
        parse_attribute_combination_string("{val}", [])

def test_parse_combinations_mismatch_groups_attributes(sample_parsed_attributes_fixture):
    combo_str = "{Black|main_sku:true}" # Only one group for two attributes
    with pytest.raises(ItemParserError, match="Mismatch between number of attribute groups"):
        parse_attribute_combination_string(combo_str, sample_parsed_attributes_fixture)

def test_parse_combinations_malformed_braces(sample_parsed_attributes_fixture):
    combo_str_no_start_brace = "Black|main_sku:true}|{S:M}"
    with pytest.raises(ItemParserError, match="must start with '{' and end with '}'"):
        parse_attribute_combination_string(combo_str_no_start_brace, sample_parsed_attributes_fixture)
    
    combo_str_no_end_brace = "{Black|main_sku:true}|{S:M"
    with pytest.raises(ItemParserError, match="must start with '{' and end with '}'"):
        parse_attribute_combination_string(combo_str_no_end_brace, sample_parsed_attributes_fixture)

def test_parse_combinations_empty_value_in_main_group(sample_parsed_attributes_fixture):
    combo_str = "{Black|main_sku:true::White|main_sku:false}|{S:M}" # Empty segment due to ::
    with pytest.raises(ItemParserError, match="Malformed value string for main attribute 'color'"): # Error because it expects pairs
        parse_attribute_combination_string(combo_str, sample_parsed_attributes_fixture)

def test_parse_combinations_empty_group_for_non_main(sample_parsed_attributes_fixture):
    combo_str = "{Black|main_sku:true}|{}"
    with pytest.raises(ItemParserError, match="Attribute group for 'size' has no values"):
         parse_attribute_combination_string(combo_str, sample_parsed_attributes_fixture)

def test_parse_combinations_main_attr_malformed_flag_pair(sample_parsed_attributes_fixture):
    combo_str = "{Black|main_sku}|{S:M}" # Missing :true/false part
    with pytest.raises(ItemParserError, match="Malformed value string for main attribute 'color'"):
        parse_attribute_combination_string(combo_str, sample_parsed_attributes_fixture)

def test_parse_combinations_main_attr_invalid_flag_value(sample_parsed_attributes_fixture):
    combo_str = "{Black|main_sku:yes}|{S:M}"
    with pytest.raises(ItemParserError, match="Invalid boolean flag 'yes' for value 'Black'"):
        parse_attribute_combination_string(combo_str, sample_parsed_attributes_fixture)

def test_parse_combinations_main_attr_missing_value_name(sample_parsed_attributes_fixture):
    combo_str = "{|main_sku:true}|{S:M}"
    with pytest.raises(ItemParserError, match="Empty actual value for main attribute 'color'"):
        parse_attribute_combination_string(combo_str, sample_parsed_attributes_fixture)

def test_parse_combinations_main_attr_malformed_name_part(sample_parsed_attributes_fixture):
    combo_str = "{Black_main_sku:true}|{S:M}" # Missing pipe in "Black|main_sku"
    with pytest.raises(ItemParserError, match="Expected 'ValueName|main_sku'"):
        parse_attribute_combination_string(combo_str, sample_parsed_attributes_fixture)

def test_parse_combinations_non_main_attr_empty_value(sample_parsed_attributes_fixture):
    combo_str = "{Black|main_sku:true}|{S::M}" # Empty value for size due to ::
    with pytest.raises(ItemParserError, match="Empty value found for non-main attribute 'size'"):
        parse_attribute_combination_string(combo_str, sample_parsed_attributes_fixture)

# --- Tests for generate_sku_variants ---

@pytest.fixture
def sample_parsed_attribute_values_for_gen(sample_parsed_attributes_fixture):
    return parse_attribute_combination_string(
        "{Black|main_sku:true:White|main_sku:false}|{S:M}",
        sample_parsed_attributes_fixture
    )

def test_generate_variants_valid(sample_parsed_attribute_values_for_gen, sample_parsed_attributes_fixture):
    variants = generate_sku_variants(sample_parsed_attribute_values_for_gen, sample_parsed_attributes_fixture)
    assert len(variants) == 4 # 2 colors * 2 sizes
    
    expected_variants_subset = [
        [{'attribute_name': 'color', 'value': 'Black', 'is_default_sku_value': True}, {'attribute_name': 'size', 'value': 'S'}],
        [{'attribute_name': 'color', 'value': 'Black', 'is_default_sku_value': True}, {'attribute_name': 'size', 'value': 'M'}],
        [{'attribute_name': 'color', 'value': 'White', 'is_default_sku_value': False}, {'attribute_name': 'size', 'value': 'S'}],
        [{'attribute_name': 'color', 'value': 'White', 'is_default_sku_value': False}, {'attribute_name': 'size', 'value': 'M'}]
    ]
    for ev in expected_variants_subset:
        assert ev in variants
    assert len(variants) == len(expected_variants_subset)

def test_generate_variants_single_attribute_set(sample_parsed_attributes_fixture):
    parsed_attrs_single = [sample_parsed_attributes_fixture[0]] # Only color
    parsed_values_single = parse_attribute_combination_string(
        "{Red|main_sku:true:Green|main_sku:false}",
        parsed_attrs_single
    )
    variants = generate_sku_variants(parsed_values_single, parsed_attrs_single)
    assert len(variants) == 2
    expected = [
        [{'attribute_name': 'color', 'value': 'Red', 'is_default_sku_value': True}],
        [{'attribute_name': 'color', 'value': 'Green', 'is_default_sku_value': False}]
    ]
    for ev in expected:
        assert ev in variants
    assert len(variants) == len(expected)

def test_generate_variants_empty_parsed_attribute_values_list(sample_parsed_attributes_fixture):
    # Test with empty list for parsed_attribute_values
    assert generate_sku_variants([], sample_parsed_attributes_fixture) == []


def test_generate_variants_mismatch_values_attributes_len(sample_parsed_attributes_fixture):
    one_group_values = [[{'value': 'Black', 'is_default_sku_value': True}]] 
    with pytest.raises(ItemParserError, match="Mismatch in length between parsed_attribute_values"):
        generate_sku_variants(one_group_values, sample_parsed_attributes_fixture)

def test_generate_variants_one_attribute_has_empty_value_list_for_it(sample_parsed_attributes_fixture):
    invalid_parsed_values = [
        [{'value': 'Black', 'is_default_sku_value': True}], 
        []  # Empty list for Size values
    ]
    with pytest.raises(ItemParserError, match="Attribute 'size' has an empty list of values"):
        generate_sku_variants(invalid_parsed_values, sample_parsed_attributes_fixture)

def test_generate_variants_three_attributes(sample_parsed_attributes_fixture):
    # color, size, material
    three_parsed_attributes = [
        {'name': 'color', 'is_main': True},
        {'name': 'size', 'is_main': False},
        {'name': 'material', 'is_main': False}
    ]
    three_parsed_values = parse_attribute_combination_string(
        "{Black|main_sku:true}|{S:M}|{Cotton:Polyester}",
        three_parsed_attributes
    )
    variants = generate_sku_variants(three_parsed_values, three_parsed_attributes)
    assert len(variants) == 4 # 1 color * 2 sizes * 2 materials
    
    # Check one specific variant
    expected_variant = [
        {'attribute_name': 'color', 'value': 'Black', 'is_default_sku_value': True},
        {'attribute_name': 'size', 'value': 'M'},
        {'attribute_name': 'material', 'value': 'Polyester'}
    ]
    # We need to find this in the list of variants. The order from itertools.product is deterministic.
    # Black, S, Cotton
    # Black, S, Polyester
    # Black, M, Cotton
    # Black, M, Polyester <- this is the one
    assert expected_variant in variants


# --- Tests for Per-Combination Data Extractors ---

@pytest.fixture
def common_test_data_for_extractors():
    # Corresponds to: color (main), size
    # Colors: Black (main_sku:true), White (main_sku:false)
    # Sizes: S, M
    parsed_attributes = [
        {'name': 'color', 'is_main': True},
        {'name': 'size', 'is_main': False}
    ]
    # Output from parse_attribute_combination_string for "{Black|main_sku:true:White|main_sku:false}|{S:M}"
    parsed_attribute_values = [
        [{'value': 'Black', 'is_default_sku_value': True}, {'value': 'White', 'is_default_sku_value': False}],
        [{'value': 'S'}, {'value': 'M'}]
    ]
    # Output from generate_sku_variants
    # Order: (Black,S), (Black,M), (White,S), (White,M)
    all_sku_variants = [
        [{'attribute_name': 'color', 'value': 'Black', 'is_default_sku_value': True}, {'attribute_name': 'size', 'value': 'S'}],
        [{'attribute_name': 'color', 'value': 'Black', 'is_default_sku_value': True}, {'attribute_name': 'size', 'value': 'M'}],
        [{'attribute_name': 'color', 'value': 'White', 'is_default_sku_value': False}, {'attribute_name': 'size', 'value': 'S'}],
        [{'attribute_name': 'color', 'value': 'White', 'is_default_sku_value': False}, {'attribute_name': 'size', 'value': 'M'}],
    ]
    return {
        "parsed_attributes": parsed_attributes,
        "parsed_attribute_values": parsed_attribute_values,
        "all_sku_variants": all_sku_variants
    }

# Test get_value_for_combination (core logic)
def test_get_value_for_combination_2_attrs_valid(common_test_data_for_extractors):
    data = common_test_data_for_extractors
    price_str = "10.00:12.00|20.00:22.00" # (Black,S):10, (Black,M):12 | (White,S):20, (White,M):22

    # Black, S -> 10.00
    val1 = parse_attributes_string.get_value_for_combination(price_str, data["parsed_attributes"], data["parsed_attribute_values"], data["all_sku_variants"][0], float, False, "price")
    assert val1 == 10.00
    # Black, M -> 12.00
    val2 = parse_attributes_string.get_value_for_combination(price_str, data["parsed_attributes"], data["parsed_attribute_values"], data["all_sku_variants"][1], float, False, "price")
    assert val2 == 12.00
    # White, S -> 20.00
    val3 = parse_attributes_string.get_value_for_combination(price_str, data["parsed_attributes"], data["parsed_attribute_values"], data["all_sku_variants"][2], float, False, "price")
    assert val3 == 20.00
    # White, M -> 22.00
    val4 = parse_attributes_string.get_value_for_combination(price_str, data["parsed_attributes"], data["parsed_attribute_values"], data["all_sku_variants"][3], float, False, "price")
    assert val4 == 22.00

def test_get_value_for_combination_1_attr_valid(common_test_data_for_extractors):
    data = common_test_data_for_extractors
    # Only use 'color' attribute for this test.
    single_attr_def = [data["parsed_attributes"][0]]
    single_attr_vals = [data["parsed_attribute_values"][0]]
    # Variants for single attribute 'color': Black, White
    single_attr_variants = [
        [data["all_sku_variants"][0][0]], # Black
        [data["all_sku_variants"][2][0]], # White
    ]
    
    qty_str = "100:150" # Black:100, White:150 (using ':' as per current logic for single attr)

    # Black -> 100
    val_black = parse_attributes_string.get_value_for_combination(qty_str, single_attr_def, single_attr_vals, single_attr_variants[0], int, False, "quantity")
    assert val_black == 100
    # White -> 150
    val_white = parse_attributes_string.get_value_for_combination(qty_str, single_attr_def, single_attr_vals, single_attr_variants[1], int, False, "quantity")
    assert val_white == 150

def test_get_value_for_combination_optional_missing(common_test_data_for_extractors):
    data = common_test_data_for_extractors
    # Price string missing data for White,M (index [1][1])
    price_str_partial = "10.00:12.00|20.00" # Only (White,S) has price, (White,M) missing
    
    # White, M -> should be None
    val_white_m = parse_attributes_string.get_value_for_combination(price_str_partial, data["parsed_attributes"], data["parsed_attribute_values"], data["all_sku_variants"][3], float, True, "price_optional")
    assert val_white_m is None

    # White, S -> should get 20.00
    val_white_s = parse_attributes_string.get_value_for_combination(price_str_partial, data["parsed_attributes"], data["parsed_attribute_values"], data["all_sku_variants"][2], float, True, "price_optional")
    assert val_white_s == 20.00


def test_get_value_for_combination_required_missing_raises_error(common_test_data_for_extractors):
    data = common_test_data_for_extractors
    price_str_partial = "10.00:12.00|20.00" # Missing for (White,M)
    with pytest.raises(ItemParserError, match="Value for SKU variant .* not found in 'price_required' data"):
        parse_attributes_string.get_value_for_combination(price_str_partial, data["parsed_attributes"], data["parsed_attribute_values"], data["all_sku_variants"][3], float, False, "price_required")

def test_get_value_for_combination_empty_data_str_optional(common_test_data_for_extractors):
    data = common_test_data_for_extractors
    val = parse_attributes_string.get_value_for_combination(None, data["parsed_attributes"], data["parsed_attribute_values"], data["all_sku_variants"][0], str, True, "desc")
    assert val is None
    val_empty = parse_attributes_string.get_value_for_combination("", data["parsed_attributes"], data["parsed_attribute_values"], data["all_sku_variants"][0], str, True, "desc")
    assert val_empty is None

def test_get_value_for_combination_empty_data_str_required_error(common_test_data_for_extractors):
    data = common_test_data_for_extractors
    with pytest.raises(ItemParserError, match="Required field 'desc_req' is missing or empty"):
        parse_attributes_string.get_value_for_combination(None, data["parsed_attributes"], data["parsed_attribute_values"], data["all_sku_variants"][0], str, False, "desc_req")

def test_get_value_for_combination_type_conversion_error(common_test_data_for_extractors):
    data = common_test_data_for_extractors
    price_str = "10.00:abc|20.00:22.00" # abc cannot be float
    with pytest.raises(ItemParserError, match="Cannot convert value 'abc' to type 'float' for field 'price'"):
        parse_attributes_string.get_value_for_combination(price_str, data["parsed_attributes"], data["parsed_attribute_values"], data["all_sku_variants"][1], float, False, "price")

def test_get_value_for_combination_malformed_primary_groups(common_test_data_for_extractors):
    data = common_test_data_for_extractors
    price_str = "10.00:12.00" # Only one primary group, expected 2 (for Black, White)
    with pytest.raises(ItemParserError, match="Expected 2 primary groups for attribute 'color'"):
        parse_attributes_string.get_value_for_combination(price_str, data["parsed_attributes"], data["parsed_attribute_values"], data["all_sku_variants"][2], float, False, "price") # Accessing White

def test_get_value_for_combination_malformed_secondary_values(common_test_data_for_extractors):
    data = common_test_data_for_extractors
    price_str = "10.00:12.00|20.00" # White group only has one value, expected 2 (for S, M)
    with pytest.raises(ItemParserError, match="Expected 2 secondary values for attribute 'size' in group '20.00'"):
         parse_attributes_string.get_value_for_combination(price_str, data["parsed_attributes"], data["parsed_attribute_values"], data["all_sku_variants"][3], float, False, "price") # Accessing White,M

def test_get_value_for_combination_empty_value_for_required_numeric(common_test_data_for_extractors):
    data = common_test_data_for_extractors
    price_str = "10.00:  |20.00:22.00" # Empty string for Black,M
    with pytest.raises(ItemParserError, match="Empty value found for required field 'price'"):
        parse_attributes_string.get_value_for_combination(price_str, data["parsed_attributes"], data["parsed_attribute_values"], data["all_sku_variants"][1], float, False, "price")

def test_get_value_for_combination_empty_value_for_optional_numeric(common_test_data_for_extractors):
    data = common_test_data_for_extractors
    price_str = "10.00:  |20.00:22.00" # Empty string for Black,M
    val = parse_attributes_string.get_value_for_combination(price_str, data["parsed_attributes"], data["parsed_attribute_values"], data["all_sku_variants"][1], float, True, "price_opt")
    assert val is None

def test_get_value_for_combination_empty_value_for_required_string(common_test_data_for_extractors):
    data = common_test_data_for_extractors
    desc_str = "Desc1: |Desc2:Desc3" # Empty string for Black,M
    # For string type, empty string is a valid value.
    val = parse_attributes_string.get_value_for_combination(desc_str, data["parsed_attributes"], data["parsed_attribute_values"], data["all_sku_variants"][1], str, False, "desc")
    assert val == ""


# --- Tests for Specific Extractor Wrappers ---

def test_get_price_for_combination(common_test_data_for_extractors):
    data = common_test_data_for_extractors
    price_str = "10.00:12.00|20.00:22.00"
    assert parse_attributes_string.get_price_for_combination(price_str, data["parsed_attributes"], data["parsed_attribute_values"], data["all_sku_variants"][0]) == 10.00

def test_get_quantity_for_combination(common_test_data_for_extractors):
    data = common_test_data_for_extractors
    qty_str = "100:120|200:220"
    assert parse_attributes_string.get_quantity_for_combination(qty_str, data["parsed_attributes"], data["parsed_attribute_values"], data["all_sku_variants"][1]) == 120

# Tests for get_status_for_combination
def test_get_status_for_combination_valid(common_test_data_for_extractors):
    data = common_test_data_for_extractors
    status_str = "ACTIVE|INACTIVE" # Black variants: ACTIVE, White variants: INACTIVE
    # Black, S (main attr 'Black' is at index 0 of its values)
    assert parse_attributes_string.get_status_for_combination(status_str, data["parsed_attributes"], data["parsed_attribute_values"], data["all_sku_variants"][0]) == "ACTIVE"
    # White, M (main attr 'White' is at index 1 of its values)
    assert parse_attributes_string.get_status_for_combination(status_str, data["parsed_attributes"], data["parsed_attribute_values"], data["all_sku_variants"][3]) == "INACTIVE"

def test_get_status_for_combination_default_active(common_test_data_for_extractors):
    data = common_test_data_for_extractors
    status_str_short = "ACTIVE" # Only one status, White variants should default
    assert parse_attributes_string.get_status_for_combination(status_str_short, data["parsed_attributes"], data["parsed_attribute_values"], data["all_sku_variants"][3]) == "ACTIVE"
    
    status_str_empty_val = "ACTIVE|" # Empty for White
    assert parse_attributes_string.get_status_for_combination(status_str_empty_val, data["parsed_attributes"], data["parsed_attribute_values"], data["all_sku_variants"][3]) == "ACTIVE"

def test_get_status_for_combination_invalid_value_error(common_test_data_for_extractors):
    data = common_test_data_for_extractors
    status_str_invalid = "ACTIVE|PENDING"
    with pytest.raises(ItemParserError, match="Invalid status value 'PENDING'"):
        parse_attributes_string.get_status_for_combination(status_str_invalid, data["parsed_attributes"], data["parsed_attribute_values"], data["all_sku_variants"][3])

# Tests for get_optional_typed_value_for_combination and its wrappers (order_limit etc.)
def test_get_order_limit_valid(common_test_data_for_extractors):
    data = common_test_data_for_extractors
    limit_str = "10|5" # Black variants: 10, White variants: 5
    assert parse_attributes_string.get_order_limit_for_combination(limit_str, data["parsed_attributes"], data["parsed_attribute_values"], data["all_sku_variants"][0]) == 10
    assert parse_attributes_string.get_order_limit_for_combination(limit_str, data["parsed_attributes"], data["parsed_attribute_values"], data["all_sku_variants"][2]) == 5

def test_get_order_limit_optional_missing(common_test_data_for_extractors):
    data = common_test_data_for_extractors
    assert parse_attributes_string.get_order_limit_for_combination(None, data["parsed_attributes"], data["parsed_attribute_values"], data["all_sku_variants"][0]) is None
    
    limit_str_short = "10" # Missing for White variants
    assert parse_attributes_string.get_order_limit_for_combination(limit_str_short, data["parsed_attributes"], data["parsed_attribute_values"], data["all_sku_variants"][2]) is None

    limit_str_empty_val = "10|" # Empty for White variants
    assert parse_attributes_string.get_order_limit_for_combination(limit_str_empty_val, data["parsed_attributes"], data["parsed_attribute_values"], data["all_sku_variants"][2]) is None

def test_get_order_limit_conversion_fail_returns_none(common_test_data_for_extractors):
    data = common_test_data_for_extractors
    limit_str_invalid = "10|abc" # abc for White variants
    assert parse_attributes_string.get_order_limit_for_combination(limit_str_invalid, data["parsed_attributes"], data["parsed_attribute_values"], data["all_sku_variants"][2]) is None

def test_get_package_weight_valid(common_test_data_for_extractors):
    data = common_test_data_for_extractors
    weight_str = "0.5|0.8"
    assert parse_attributes_string.get_package_weight_for_combination(weight_str, data["parsed_attributes"], data["parsed_attribute_values"], data["all_sku_variants"][3]) == 0.8

# Test for 3 attributes (NotImplementedError)
def test_get_value_for_3_attrs_not_implemented(common_test_data_for_extractors):
    data = common_test_data_for_extractors
    three_attrs = data["parsed_attributes"] + [{'name': 'material', 'is_main': False}]
    three_attr_vals = data["parsed_attribute_values"] + [[{'value': 'Cotton'}]]
    three_attr_variant = data["all_sku_variants"][0] + [{'attribute_name': 'material', 'value': 'Cotton'}]
    
    data_str = "val1|val2" # Dummy string
    with pytest.raises(NotImplementedError, match="Parsing for 3 attributes for field 'test_field' is not implemented"):
        parse_attributes_string.get_value_for_combination(data_str, three_attrs, three_attr_vals, three_attr_variant, str, False, "test_field")