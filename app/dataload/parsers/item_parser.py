from typing import List, Dict, Any, Optional # Added Optional

class ItemParserError(ValueError):
    """Custom exception for item parsing errors."""
    pass

def parse_attributes_string(attributes_str: str) -> List[Dict[str, Any]]:
    """
    Parses the attributes string from the CSV.
    Example input: "color|main_attribute:true|size|main_attribute:false"
    Example output: [{'name': 'color', 'is_main': True}, {'name': 'size', 'is_main': False}]
    """
    if not attributes_str:
        raise ItemParserError("Attributes string cannot be empty.")

    parts = attributes_str.split('|')
    if not parts: # Should not happen if attributes_str is not empty, but defensive.
        raise ItemParserError("Attributes string is malformed (empty after split).")

    parsed_attributes: List[Dict[str, Any]] = []
    main_attribute_is_true_found = False # Tracks if a 'main_attribute:true' is found
    
    if len(parts) % 2 != 0:
        raise ItemParserError(
            f"Attributes string '{attributes_str}' is malformed. "
            "Expected pairs of attribute name and main_attribute flag. Must have an even number of segments."
        )
    if not parts[0].strip(): # Check if the first part (potential attribute name) is empty
         raise ItemParserError(f"First attribute name cannot be empty in '{attributes_str}'.")


    for i in range(0, len(parts), 2):
        attr_name = parts[i].strip()
        
        # Ensure we don't try to access parts[i+1] if it's out of bounds (already checked by len(parts)%2!=0)
        # but good to be defensive if loop structure were different.
        flag_str_original = parts[i+1] 
        flag_str = flag_str_original.strip().lower()

        if not attr_name:
            raise ItemParserError(f"Attribute name cannot be empty in '{attributes_str}'. Found at pair index {i//2}.")

        is_main_flag_value = None # Represents the boolean value of the main_attribute flag
        if flag_str == "main_attribute:true":
            is_main_flag_value = True
        elif flag_str == "main_attribute:false":
            is_main_flag_value = False
        else:
            raise ItemParserError(
                f"Invalid main_attribute flag '{flag_str_original}' for attribute '{attr_name}'. "
                "Expected 'main_attribute:true' or 'main_attribute:false'."
            )

        if is_main_flag_value: # If this attribute is marked as main_attribute:true
            if main_attribute_is_true_found:
                # This is the error: more than one attribute is marked main_attribute:true
                raise ItemParserError("Multiple main attributes defined. Only one attribute can be 'main_attribute:true'.")
            main_attribute_is_true_found = True
        
        parsed_attributes.append({'name': attr_name, 'is_main': is_main_flag_value})

    if not parsed_attributes: 
        # This case should ideally be caught by len(parts) % 2 != 0 or if attributes_str was empty.
        # If attributes_str was, for example, "||||", parts would be non-empty but attr_names would be empty.
        raise ItemParserError("No attributes could be parsed from the input string.")

    # Validation: "only one attribute can be the main attribute" (meaning main_attribute:true)
    # The loop already checks for multiple 'main_attribute:true'.
    # Now, ensure that if there are attributes, at least one is marked 'main_attribute:true',
    # as per the example "color|main_attribute:true|size|main_attribute:false" which implies one MUST be true.
    # This also simplifies downstream logic which expects one clear main attribute for pivoting.
    if parsed_attributes and not main_attribute_is_true_found:
        raise ItemParserError("No attribute was marked as 'main_attribute:true'. One attribute must be designated as the main attribute.")

    return parsed_attributes


def parse_attribute_combination_string(
    attr_combination_str: str,
    parsed_attributes: List[Dict[str, Any]] # Output from parse_attributes_string
) -> List[List[Dict[str, Any]]]:
    """
    Parses the attribute_combination string from the CSV 
    (e.g., "{Black|main_sku:true:White|main_sku:false}|{S:M:L:XL}")
    using the definitions from parsed_attributes.

    Output: A list of lists, where each inner list contains value details for an attribute.
            The order matches parsed_attributes.
            [
                [{'value': 'Black', 'is_default_sku_value': True}, {'value': 'White', 'is_default_sku_value': False}, ...],
                [{'value': 'S'}, {'value': 'M'}, ...]
            ]
    """
    if not attr_combination_str:
        raise ItemParserError("Attribute combination string cannot be empty.")
    if not parsed_attributes:
        raise ItemParserError("Parsed attributes list cannot be empty for parsing combinations.")

    # Split by '}|{' to separate attribute groups, then clean up braces from first/last group.
    stripped_str = attr_combination_str.strip()
    if not (stripped_str.startswith('{') and stripped_str.endswith('}')):
        raise ItemParserError(
            f"Attribute combination string '{attr_combination_str}' must start with '{{' and end with '}}'."
        )

    # Remove outermost braces for splitting: e.g. "{groupA}|{groupB}" -> "groupA}|{groupB"
    content_str = stripped_str[1:-1]
    raw_groups = content_str.split('}|{')

    if not raw_groups: # Should not happen if content_str was not empty
        raise ItemParserError("No attribute value groups found in combination string after stripping braces.")
            
    if len(raw_groups) != len(parsed_attributes):
        raise ItemParserError(
            f"Mismatch between number of attribute groups in combination string ({len(raw_groups)}) "
            f"and number of defined attributes ({len(parsed_attributes)})."
        )

    result_list: List[List[Dict[str, Any]]] = []
    
    for i, group_str in enumerate(raw_groups):
        attribute_definition = parsed_attributes[i]
        current_attribute_is_main = attribute_definition['is_main']
        values_for_this_attribute: List[Dict[str, Any]] = []

        # Values within a group are separated by ':'
        value_segments = group_str.split(':')
        
        # current_attribute_is_main = attribute_definition['is_main'] # This is NOT used to determine parsing style for segments

        # Values within a group are separated by ':'.
        # All attribute values are expected to follow "ValueName|main_sku:boolean_flag" structure.
        raw_value_segments = group_str.split(':')
        # Filter out empty strings that might result from leading/trailing/double colons, and strip valid ones.
        value_segments = [segment.strip() for segment in raw_value_segments if segment.strip()]

        if not value_segments:
            original_content_present = group_str.strip() # Check original group_str before it was split
            if not original_content_present: # group_str was empty or just whitespace
                raise ItemParserError(
                    f"Attribute group for '{attribute_definition['name']}' is empty."
                )
            # If original_content_present is true, but value_segments is empty,
            # it means group_str consisted only of colons or colons and whitespace.
            raise ItemParserError(
                f"Attribute group for '{attribute_definition['name']}' ('{group_str}') "
                "resulted in no valid segments after processing colons. Check for excessive or misplaced colons."
            )

        if len(value_segments) % 2 != 0:
            raise ItemParserError(
                f"Malformed value string for attribute '{attribute_definition['name']}': '{group_str}'. "
                f"Processed segments: {value_segments}. Expected an even number of segments for 'ValueName|main_sku' and 'true/false' pairs."
            )
        
        for vp_idx in range(0, len(value_segments), 2):
            val_name_part_raw = value_segments[vp_idx] 
            val_flag_part_raw = value_segments[vp_idx+1]

            val_name_part = val_name_part_raw 
            val_flag_part = val_flag_part_raw.lower()
            
            expected_suffix = "|main_sku"
            if not val_name_part.lower().endswith(expected_suffix.lower()):
                raise ItemParserError(
                    f"Malformed value name part '{val_name_part_raw}' for attribute "
                    f"'{attribute_definition['name']}'. Expected it to end with '{expected_suffix}' (case-insensitive)."
                )
            
            actual_value = val_name_part[:-len(expected_suffix)].strip() 
            if not actual_value: 
                 raise ItemParserError(
                     f"Empty actual value for attribute '{attribute_definition['name']}' from segment '{val_name_part_raw}' "
                     f"(after stripping '{expected_suffix}')."
                 )

            value_detail = {'value': actual_value}
            if val_flag_part == "true":
                value_detail['is_default_sku_value'] = True
            elif val_flag_part == "false":
                value_detail['is_default_sku_value'] = False
            else:
                raise ItemParserError(
                    f"Invalid boolean flag component '{val_flag_part_raw}' for value '{actual_value}' "
                    f"of attribute '{attribute_definition['name']}'. Expected 'true' or 'false'."
                )
            values_for_this_attribute.append(value_detail)
        
        if not values_for_this_attribute: 
             raise ItemParserError(f"No values could be parsed for attribute '{attribute_definition['name']}' from segment '{group_str}'.")
        result_list.append(values_for_this_attribute)
            
    return result_list


import itertools

def generate_sku_variants(
    parsed_attribute_values: List[List[Dict[str, Any]]], # Output from parse_attribute_combination_string
    parsed_attributes: List[Dict[str, Any]] # Output from parse_attributes_string (to get names)
) -> List[List[Dict[str, Any]]]:
    """
    Generates all unique SKU variant combinations (Cartesian product) from parsed attribute values.
    
    Input parsed_attribute_values example: 
    [
        [{'value': 'Black', 'is_default_sku_value': True}, {'value': 'White', 'is_default_sku_value': False}], # Color values
        [{'value': 'S'}, {'value': 'M'}]  # Size values
    ]
    Input parsed_attributes example:
    [{'name': 'color', 'is_main': True}, {'name': 'size', 'is_main': False}]

    Output: A list of SKU variants. Each variant is a list of attribute details.
    [
        [ {'attribute_name': 'color', 'value': 'Black', 'is_default_sku_value': True}, {'attribute_name': 'size', 'value': 'S'} ],
        [ {'attribute_name': 'color', 'value': 'Black', 'is_default_sku_value': True}, {'attribute_name': 'size', 'value': 'M'} ],
        [ {'attribute_name': 'color', 'value': 'White', 'is_default_sku_value': False}, {'attribute_name': 'size', 'value': 'S'} ],
        # ... and so on
    ]
    """
    if not parsed_attribute_values:
        # If there are no attribute value lists (e.g. product has no attributes defined in CSV)
        # Return a list containing one "empty" variant if this represents a simple product.
        # However, given the CSV structure, this function will likely always receive non-empty parsed_attribute_values
        # if the input CSV row is valid up to this point.
        # If parsed_attributes is empty, parse_attribute_combination_string should ideally handle it or raise error.
        # For now, let's assume valid, non-empty inputs as per this function's direct role.
        # If a product truly has no attributes, it might not go through this item/variant loading path.
        return [] # Or raise error, depending on how products with no variants are handled.

    if len(parsed_attribute_values) != len(parsed_attributes):
        # This check ensures consistency between the attribute definitions and the provided values.
        raise ItemParserError(
            f"Mismatch in length between parsed_attribute_values ({len(parsed_attribute_values)}) "
            f"and parsed_attributes ({len(parsed_attributes)})."
        )
    
    # Check if any list of attribute values is empty, which would result in an empty product set.
    for i, values_list in enumerate(parsed_attribute_values):
        if not values_list:
            attr_name = parsed_attributes[i]['name'] if i < len(parsed_attributes) else f"index {i}"
            raise ItemParserError(
                f"Attribute '{attr_name}' has an empty list of values. Cannot generate variants."
            )

    # The list of lists of values (parsed_attribute_values) is what itertools.product needs.
    all_combinations_tuples = list(itertools.product(*parsed_attribute_values))

    sku_variants_list: List[List[Dict[str, Any]]] = []

    for combo_tuple in all_combinations_tuples:
        current_variant_details: List[Dict[str, Any]] = []
        if len(combo_tuple) != len(parsed_attributes):
            # This should not happen if itertools.product works as expected and inputs are consistent.
            raise ItemParserError(
                "Internal error: Combination tuple length does not match attribute count."
            )
            
        for i, value_dict_for_attr in enumerate(combo_tuple):
            attribute_name = parsed_attributes[i]['name']
            # Create a new dict to store attribute name along with value details
            # value_dict_for_attr is like {'value': 'Black', 'is_default_sku_value': True} or {'value': 'S'}
            attr_detail_for_variant = {'attribute_name': attribute_name, **value_dict_for_attr}
            current_variant_details.append(attr_detail_for_variant)
        sku_variants_list.append(current_variant_details)
        
    # It's possible all_combinations_tuples is empty if one of the input lists in parsed_attribute_values was empty.
    # This is now checked before itertools.product.
    # If sku_variants_list is empty here, it means parsed_attribute_values was empty or contained an empty list,
    # which should have been caught or handled.
            
    return sku_variants_list


# --- Per-Combination Data Extractors ---

def get_value_for_combination(
    data_str: Optional[str],
    parsed_attributes: List[Dict[str, Any]], # Defines order and names of attributes
    parsed_attribute_values: List[List[Dict[str, Any]]], # List of value lists for each attribute type
    current_sku_variant: List[Dict[str, Any]], # The specific variant: [{'attr_name':'color', 'value':'Red'}, ...]
    expected_type: type,
    is_optional: bool,
    field_name_for_error: str,
    delimiters: List[str] = ['|', ':'] # Delimiter for 1st attribute's groups, 2nd attribute's values, etc.
) -> Any:
    """
    Retrieves and type-converts a specific value for a given SKU variant from a complex delimited string.
    Handles up to 2 attributes based on the provided delimiters.
    """
    if data_str is None or not data_str.strip():
        if is_optional:
            return None
        else:
            # Provide more context if possible, like product name or current SKU variant description
            variant_desc_parts = [f"{v['attribute_name']}:{v['value']}" for v in current_sku_variant]
            variant_desc = ", ".join(variant_desc_parts)
            raise ItemParserError(
                f"Required field '{field_name_for_error}' is missing or empty in CSV data "
                f"for SKU variant ({variant_desc})."
            )

    num_attributes = len(parsed_attributes)
    if num_attributes == 0:
        if is_optional: return None # Or handle as single value if data_str is simple
        raise ItemParserError(f"Cannot get value for '{field_name_for_error}': No attributes defined for variant construction.")

    target_indices = []
    for i in range(num_attributes):
        attr_name_from_def = parsed_attributes[i]['name']
        variant_attr_detail = next((vad for vad in current_sku_variant if vad['attribute_name'] == attr_name_from_def), None)
        
        if not variant_attr_detail:
            # This should not happen if current_sku_variant is correctly generated by generate_sku_variants
            raise ItemParserError(f"Internal error: Attribute '{attr_name_from_def}' not found in current_sku_variant while parsing '{field_name_for_error}'.")
        
        target_value = variant_attr_detail['value']
        values_for_this_attr_type = parsed_attribute_values[i]
        
        try:
            idx = next(j for j, val_dict in enumerate(values_for_this_attr_type) if val_dict['value'] == target_value)
            target_indices.append(idx)
        except StopIteration:
            raise ItemParserError(
                f"Internal error: Value '{target_value}' for attribute '{attr_name_from_def}' "
                f"not found in its definition list while parsing '{field_name_for_error}'."
            )
    
    raw_value_str = ""
    try:
        if num_attributes == 1:
            # Values are directly in data_str, separated by the *last* conventional delimiter (e.g., ':')
            # or the first if only one is relevant. Let's assume ':' for single attribute value lists.
            # This implies the CSV for single-attribute price/qty would be "10:20:30" not "10|20|30"
            # The CSV example "19.99:19.99:21.99:21.99|..." uses ':' for the *second* attribute.
            # If there's only one attribute, its values should be split by the first delimiter intended for values, typically ':'.
            value_delimiter = delimiters[1] if len(delimiters) > 1 else delimiters[0] # Default to ':' or first delimiter
            
            values = data_str.split(value_delimiter)
            if len(values) != len(parsed_attribute_values[0]):
                 raise ItemParserError(
                     f"Field '{field_name_for_error}': Expected {len(parsed_attribute_values[0])} values for attribute "
                     f"'{parsed_attributes[0]['name']}', got {len(values)} from data '{data_str}'."
                 )
            raw_value_str = values[target_indices[0]]

        elif num_attributes == 2:
            primary_groups = data_str.split(delimiters[0]) # Split by '|' for 1st attribute groups
            if len(primary_groups) != len(parsed_attribute_values[0]):
                 raise ItemParserError(
                     f"Field '{field_name_for_error}': Expected {len(parsed_attribute_values[0])} primary groups for attribute "
                     f"'{parsed_attributes[0]['name']}', got {len(primary_groups)} from data '{data_str}'."
                 )
            secondary_group_str = primary_groups[target_indices[0]]

            secondary_values = secondary_group_str.split(delimiters[1]) # Split by ':' for 2nd attribute values
            if len(secondary_values) != len(parsed_attribute_values[1]):
                raise ItemParserError(
                    f"Field '{field_name_for_error}': Expected {len(parsed_attribute_values[1])} secondary values for attribute "
                    f"'{parsed_attributes[1]['name']}' in group '{secondary_group_str}', got {len(secondary_values)}."
                )
            raw_value_str = secondary_values[target_indices[1]]
        else:
            raise NotImplementedError(
                f"Parsing for {num_attributes} attributes for field '{field_name_for_error}' is not implemented. "
                "This helper currently supports 1 or 2 attributes."
            )
    except IndexError: # Handles if target_indices are out of bounds for the parsed data segments
        if is_optional:
            return None
        variant_desc_parts = [f"{v['attribute_name']}:{v['value']}" for v in current_sku_variant]
        variant_desc = ", ".join(variant_desc_parts)
        raise ItemParserError(
            f"Value for SKU variant ({variant_desc}) not found in '{field_name_for_error}' data. "
            f"Calculated indices {target_indices} might be out of range for data string '{data_str}'."
        )

    raw_value_str = raw_value_str.strip()
    if not raw_value_str:
        if is_optional:
            return None
        if expected_type != str: # For non-string types, an empty string after strip is usually an error if required.
            variant_desc_parts = [f"{v['attribute_name']}:{v['value']}" for v in current_sku_variant]
            variant_desc = ", ".join(variant_desc_parts)
            raise ItemParserError(
                f"Empty value found for required field '{field_name_for_error}' for SKU variant ({variant_desc})."
            )
        # If expected_type is str, an empty string might be a valid value.

    try:
        if expected_type == str:
            return raw_value_str
        if expected_type == int:
            return int(raw_value_str)
        if expected_type == float:
            return float(raw_value_str)
        # Add bool if 'ACTIVE'/'INACTIVE' or '1'/'0' needs to be bool, though status is usually string.
        # Example for boolean from 'active'/'inactive' strings:
        # if expected_type == bool and isinstance(raw_value_str, str):
        #     if raw_value_str.upper() == 'ACTIVE': return True
        #     if raw_value_str.upper() == 'INACTIVE': return False
        #     raise ValueError("Invalid boolean string")

    except ValueError as e:
        variant_desc_parts = [f"{v['attribute_name']}:{v['value']}" for v in current_sku_variant]
        variant_desc = ", ".join(variant_desc_parts)
        raise ItemParserError(
            f"Cannot convert value '{raw_value_str}' to type '{expected_type.__name__}' "
            f"for field '{field_name_for_error}' for SKU variant ({variant_desc}). Original error: {e}"
        )
    
    # Should not be reached if expected_type is handled above
    raise ItemParserError(f"Unhandled expected_type '{expected_type.__name__}' in get_value_for_combination.")

# Now, the specific wrapper functions:

def get_price_for_combination(
    price_data_str: str, 
    parsed_attributes: List[Dict[str, Any]], 
    parsed_attribute_values: List[List[Dict[str, Any]]], 
    current_sku_variant: List[Dict[str, Any]]
) -> float:
    # Price is considered required for each variant.
    value = get_value_for_combination(
        price_data_str, parsed_attributes, parsed_attribute_values, 
        current_sku_variant, float, is_optional=False, field_name_for_error="price"
    )
    if not isinstance(value, float): # Should be caught by get_value_for_combination's type check or error
        raise ItemParserError(f"Price for variant was not a float: {value}") # Defensive
    return value

def get_quantity_for_combination(
    quantity_data_str: str, 
    parsed_attributes: List[Dict[str, Any]], 
    parsed_attribute_values: List[List[Dict[str, Any]]], 
    current_sku_variant: List[Dict[str, Any]]
) -> int:
    # Quantity is considered required.
    value = get_value_for_combination(
        quantity_data_str, parsed_attributes, parsed_attribute_values, 
        current_sku_variant, int, is_optional=False, field_name_for_error="quantity"
    )
    if not isinstance(value, int):
        raise ItemParserError(f"Quantity for variant was not an int: {value}")
    return value

def get_status_for_combination(
    status_data_str: str, 
    parsed_attributes: List[Dict[str, Any]], 
    parsed_attribute_values: List[List[Dict[str, Any]]], 
    current_sku_variant: List[Dict[str, Any]]
) -> str:
    # Status is string 'ACTIVE' or 'INACTIVE'. Default is 'ACTIVE'.
    # The CSV format for status is "ACTIVE|ACTIVE|ACTIVE|ACTIVE" (one per main attribute value, not per SKU)
    # This means all SKUs under a main attribute value (e.g. all "Black" SKUs) share the same status.
    # So, we only need the index of the main attribute's value.
    
    main_attr_index_in_parsed_attributes = -1
    main_attr_value_for_current_sku = ""

    for i, attr_def in enumerate(parsed_attributes):
        if attr_def['is_main']:
            main_attr_index_in_parsed_attributes = i
            # Find the value of this main attribute in the current_sku_variant
            sku_attr_detail = next(vad for vad in current_sku_variant if vad['attribute_name'] == attr_def['name'])
            main_attr_value_for_current_sku = sku_attr_detail['value']
            break
    
    if main_attr_index_in_parsed_attributes == -1:
        raise ItemParserError("Internal: Could not find main attribute definition for status parsing.")

    # Find the index of this main_attr_value_for_current_sku within its own list of possible values
    main_attr_all_possible_values = parsed_attribute_values[main_attr_index_in_parsed_attributes]
    try:
        status_group_index = next(
            j for j, val_dict in enumerate(main_attr_all_possible_values) 
            if val_dict['value'] == main_attr_value_for_current_sku
        )
    except StopIteration:
         raise ItemParserError(f"Internal: Could not find main attribute value '{main_attr_value_for_current_sku}' in its definition for status parsing.")

    status_groups = status_data_str.split('|')
    if status_group_index >= len(status_groups):
        # Not enough status entries for all main attribute values. Default to ACTIVE.
        return "ACTIVE" 
    
    status_val = status_groups[status_group_index].strip().upper()
    if not status_val: # Empty status for this group, default to ACTIVE
        return "ACTIVE"
    if status_val not in ["ACTIVE", "INACTIVE"]:
        raise ItemParserError(f"Invalid status value '{status_groups[status_group_index]}' found for variant. Expected 'ACTIVE' or 'INACTIVE'.")
    return status_val


def get_optional_typed_value_for_combination(
    data_str: Optional[str],
    parsed_attributes: List[Dict[str, Any]], 
    parsed_attribute_values: List[List[Dict[str, Any]]], 
    current_sku_variant: List[Dict[str, Any]],
    expected_type: type,
    field_name: str,
    # For optional fields like order_limit, the CSV format is "10|10|10|10" (one per main attr value)
    # similar to status.
    is_per_main_attribute_value: bool = False # If true, data_str is indexed by main_attr_value_index
) -> Any:
    if data_str is None or not data_str.strip():
        return None

    if is_per_main_attribute_value:
        main_attr_index_in_parsed_attributes = -1
        main_attr_value_for_current_sku = ""
        for i, attr_def in enumerate(parsed_attributes):
            if attr_def['is_main']:
                main_attr_index_in_parsed_attributes = i
                sku_attr_detail = next(vad for vad in current_sku_variant if vad['attribute_name'] == attr_def['name'])
                main_attr_value_for_current_sku = sku_attr_detail['value']
                break
        if main_attr_index_in_parsed_attributes == -1: raise ItemParserError("No main attr for optional field.")
        
        main_attr_all_possible_values = parsed_attribute_values[main_attr_index_in_parsed_attributes]
        try:
            target_idx = next(
                j for j, val_dict in enumerate(main_attr_all_possible_values) 
                if val_dict['value'] == main_attr_value_for_current_sku
            )
        except StopIteration: raise ItemParserError(f"Main attr value {main_attr_value_for_current_sku} not found for {field_name}")

        value_groups = data_str.split('|')
        if target_idx >= len(value_groups): return None # Not enough entries, so optional value is None
        
        raw_value_str = value_groups[target_idx].strip()
        if not raw_value_str: return None

        try:
            if expected_type == str: return raw_value_str
            if expected_type == int: return int(raw_value_str)
            if expected_type == float: return float(raw_value_str)
        except ValueError:
            # Log this or handle as warning, return None as it's optional
            return None 
        return None # Fallback for unhandled type
    else: # Value is per SKU combination
        return get_value_for_combination(
            data_str, parsed_attributes, parsed_attribute_values,
            current_sku_variant, expected_type, is_optional=True, field_name_for_error=field_name
        )

# Wrappers for optional fields that are per main attribute value
def get_order_limit_for_combination( # Corrected: order_limit is per main_attribute_value
    data_str: Optional[str], parsed_attributes: List[Dict[str, Any]], 
    parsed_attribute_values: List[List[Dict[str, Any]]], current_sku_variant: List[Dict[str, Any]]
) -> Optional[int]:
    return get_optional_typed_value_for_combination(
        data_str, parsed_attributes, parsed_attribute_values, current_sku_variant,
        int, "order_limit", is_per_main_attribute_value=True
    )

def get_package_size_length_for_combination( # Corrected: package fields are per main_attribute_value
    data_str: Optional[str], parsed_attributes: List[Dict[str, Any]], 
    parsed_attribute_values: List[List[Dict[str, Any]]], current_sku_variant: List[Dict[str, Any]]
) -> Optional[float]:
    return get_optional_typed_value_for_combination(
        data_str, parsed_attributes, parsed_attribute_values, current_sku_variant,
        float, "package_size_length", is_per_main_attribute_value=True
    )

def get_package_size_width_for_combination(
    data_str: Optional[str], parsed_attributes: List[Dict[str, Any]], 
    parsed_attribute_values: List[List[Dict[str, Any]]], current_sku_variant: List[Dict[str, Any]]
) -> Optional[float]:
    return get_optional_typed_value_for_combination(
        data_str, parsed_attributes, parsed_attribute_values, current_sku_variant,
        float, "package_size_width", is_per_main_attribute_value=True
    )

def get_package_size_height_for_combination(
    data_str: Optional[str], parsed_attributes: List[Dict[str, Any]], 
    parsed_attribute_values: List[List[Dict[str, Any]]], current_sku_variant: List[Dict[str, Any]]
) -> Optional[float]:
    return get_optional_typed_value_for_combination(
        data_str, parsed_attributes, parsed_attribute_values, current_sku_variant,
        float, "package_size_height", is_per_main_attribute_value=True
    )

def get_package_weight_for_combination( # Corrected: package_weight is per main_attribute_value
    data_str: Optional[str], parsed_attributes: List[Dict[str, Any]], 
    parsed_attribute_values: List[List[Dict[str, Any]]], current_sku_variant: List[Dict[str, Any]]
) -> Optional[float]:
    return get_optional_typed_value_for_combination(
        data_str, parsed_attributes, parsed_attribute_values, current_sku_variant,
        float, "package_weight", is_per_main_attribute_value=True
    )

# Note: Discount price was not in the CSV sample for items, but is in SKU DDL.
# If it were added to CSV with per-combination values, it would be like price/quantity.
# If it's optional:
# def get_discount_price_for_combination(...) -> Optional[float]:
#     return get_value_for_combination(..., float, is_optional=True, ...)

# Image parsing is handled separately as it's one string for the whole product row,
# not per combination in the CSV.
