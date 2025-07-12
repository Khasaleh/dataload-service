from typing import List, Dict, Any, Optional

class ItemParserError(ValueError):
    """Custom exception for item parsing errors."""
    pass

def parse_attributes_string(attributes_str: str) -> List[Dict[str, Any]]:
    if not attributes_str:
        raise ItemParserError("Attributes string cannot be empty.")

    parts = attributes_str.split('|')
    if len(parts) % 2 != 0:
        raise ItemParserError("Attributes string is malformed.")

    parsed_attributes = []
    main_attribute_found = False
    for i in range(0, len(parts), 2):
        name = parts[i].strip()
        main_attr_str = parts[i+1].strip()
        if not name or not main_attr_str.startswith("main_attribute:"):
            raise ItemParserError("Invalid attribute format.")
        
        is_main = main_attr_str.split(':')[1] == 'true'
        if is_main:
            if main_attribute_found:
                raise ItemParserError("Multiple main attributes defined.")
            main_attribute_found = True

        parsed_attributes.append({'name': name, 'is_main': is_main})

    if not main_attribute_found and parsed_attributes:
        raise ItemParserError("No main attribute defined.")
        
    return parsed_attributes

def parse_attribute_combination_string(
    attr_combination_str: str,
    parsed_attributes: List[Dict[str, Any]]
) -> List[List[Dict[str, Any]]]:
    groups = attr_combination_str.strip()[1:-1].split('}|{')
    if len(groups) != len(parsed_attributes):
        raise ItemParserError("Mismatch in attribute groups.")

    result = []
    for i, group in enumerate(groups):
        values = []
        parts = group.split(':')
        for part in parts:
            if '|' in part:
                value, main_sku_str = part.split('|')
                is_default = main_sku_str.split(':')[1] == 'true'
                values.append({'value': value.strip(), 'is_default_sku_value': is_default})
            else:
                values.append({'value': part.strip()})
        result.append(values)
    return result

import itertools

def generate_sku_variants(
    parsed_attribute_values: List[List[Dict[str, Any]]],
    parsed_attributes: List[Dict[str, Any]]
) -> List[List[Dict[str, Any]]]:
    if not parsed_attribute_values:
        return []

    combinations = list(itertools.product(*parsed_attribute_values))
    
    sku_variants = []
    for combo in combinations:
        variant_details = []
        for i, value_dict in enumerate(combo):
            attr_name = parsed_attributes[i]['name']
            detail = {'attribute_name': attr_name, **value_dict}
            variant_details.append(detail)
        sku_variants.append(variant_details)
        
    return sku_variants

def get_value_for_combination(
    data_str: Optional[str],
    parsed_attributes: List[Dict[str, Any]],
    parsed_attribute_values: List[List[Dict[str, Any]]],
    current_sku_variant: List[Dict[str, Any]],
    expected_type: type,
    is_optional: bool,
    field_name_for_error: str,
    delimiters: List[str] = ['|', ':']
) -> Any:
    if not data_str:
        if is_optional:
            return None
        raise ItemParserError(f"Required field '{field_name_for_error}' is missing.")

    num_attributes = len(parsed_attributes)
    if num_attributes == 0:
        return expected_type(data_str)

    indices = []
    for i in range(num_attributes):
        attr_name = parsed_attributes[i]['name']
        variant_value = next(v['value'] for v in current_sku_variant if v['attribute_name'] == attr_name)
        values_for_attr = [v['value'] for v in parsed_attribute_values[i]]
        indices.append(values_for_attr.index(variant_value))

    parts = data_str.split(delimiters[0])
    if num_attributes == 1:
        values = parts[0].split(delimiters[1])
        raw_value = values[indices[0]]
    elif num_attributes == 2:
        group = parts[indices[0]]
        values = group.split(delimiters[1])
        raw_value = values[indices[1]]
    else:
        raise NotImplementedError("Parsing for more than 2 attributes is not implemented.")

    return expected_type(raw_value.strip())

def get_price_for_combination(
    price_data_str: str, 
    parsed_attributes: List[Dict[str, Any]], 
    parsed_attribute_values: List[List[Dict[str, Any]]], 
    current_sku_variant: List[Dict[str, Any]]
) -> float:
    return get_value_for_combination(
        price_data_str, parsed_attributes, parsed_attribute_values, 
        current_sku_variant, float, is_optional=False, field_name_for_error="price"
    )

def get_quantity_for_combination(
    quantity_data_str: str, 
    parsed_attributes: List[Dict[str, Any]], 
    parsed_attribute_values: List[List[Dict[str, Any]]], 
    current_sku_variant: List[Dict[str, Any]]
) -> int:
    return get_value_for_combination(
        quantity_data_str, parsed_attributes, parsed_attribute_values, 
        current_sku_variant, int, is_optional=False, field_name_for_error="quantity"
    )

def get_status_for_combination(
    status_data_str: Optional[str],
    parsed_attributes: List[Dict[str, Any]], 
    parsed_attribute_values: List[List[Dict[str, Any]]], 
    current_sku_variant: List[Dict[str, Any]]
) -> str:
    if not status_data_str:
        return "ACTIVE"
    
    main_attr_index = next((i for i, attr in enumerate(parsed_attributes) if attr['is_main']), -1)
    if main_attr_index == -1:
        return "ACTIVE"

    main_attr_value = next(v['value'] for v in current_sku_variant if v['attribute_name'] == parsed_attributes[main_attr_index]['name'])
    main_attr_values_list = [v['value'] for v in parsed_attribute_values[main_attr_index]]
    value_index = main_attr_values_list.index(main_attr_value)

    statuses = status_data_str.split('|')
    if value_index < len(statuses):
        return statuses[value_index].strip().upper()
    return "ACTIVE"

def get_optional_value(
    data_str: Optional[str],
    parsed_attributes: List[Dict[str, Any]], 
    parsed_attribute_values: List[List[Dict[str, Any]]], 
    current_sku_variant: List[Dict[str, Any]],
    expected_type: type,
    field_name: str
) -> Any:
    main_attr_index = next((i for i, attr in enumerate(parsed_attributes) if attr['is_main']), -1)
    if main_attr_index == -1 or not data_str:
        return None

    main_attr_value = next(v['value'] for v in current_sku_variant if v['attribute_name'] == parsed_attributes[main_attr_index]['name'])
    main_attr_values_list = [v['value'] for v in parsed_attribute_values[main_attr_index]]
    value_index = main_attr_values_list.index(main_attr_value)

    values = data_str.split('|')
    if value_index < len(values):
        try:
            return expected_type(values[value_index].strip())
        except (ValueError, IndexError):
            return None
    return None

def get_order_limit_for_combination(
    data_str: Optional[str], parsed_attributes: List[Dict[str, Any]], 
    parsed_attribute_values: List[List[Dict[str, Any]]], current_sku_variant: List[Dict[str, Any]]
) -> Optional[int]:
    return get_optional_value(data_str, parsed_attributes, parsed_attribute_values, current_sku_variant, int, "order_limit")

def get_package_size_length_for_combination(
    data_str: Optional[str], parsed_attributes: List[Dict[str, Any]], 
    parsed_attribute_values: List[List[Dict[str, Any]]], current_sku_variant: List[Dict[str, Any]]
) -> Optional[float]:
    return get_optional_value(data_str, parsed_attributes, parsed_attribute_values, current_sku_variant, float, "package_size_length")

def get_package_size_width_for_combination(
    data_str: Optional[str], parsed_attributes: List[Dict[str, Any]], 
    parsed_attribute_values: List[List[Dict[str, Any]]], current_sku_variant: List[Dict[str, Any]]
) -> Optional[float]:
    return get_optional_value(data_str, parsed_attributes, parsed_attribute_values, current_sku_variant, float, "package_size_width")

def get_package_size_height_for_combination(
    data_str: Optional[str], parsed_attributes: List[Dict[str, Any]], 
    parsed_attribute_values: List[List[Dict[str, Any]]], current_sku_variant: List[Dict[str, Any]]
) -> Optional[float]:
    return get_optional_value(data_str, parsed_attributes, parsed_attribute_values, current_sku_variant, float, "package_size_height")

def get_package_weight_for_combination(
    data_str: Optional[str], parsed_attributes: List[Dict[str, Any]], 
    parsed_attribute_values: List[List[Dict[str, Any]]], current_sku_variant: List[Dict[str, Any]]
) -> Optional[float]:
    return get_optional_value(data_str, parsed_attributes, parsed_attribute_values, current_sku_variant, float, "package_weight")

def parse_images_string(images_str: str) -> List[Dict[str, Any]]:
    """
    Parses the images string from the CSV.
    """
    if not images_str:
        return []

    parsed_images = []
    # The string is a complex nested structure, simplify the parsing logic
    # This is a placeholder for the actual parsing logic
    # Example: "{url1|main_image:true|url2|main_image:false}|{...}"

    # Simple split logic, assuming the format is consistent
    image_groups = images_str.strip()[1:-1].split('}|{')
    for group in image_groups:
        parts = group.split('|')
        for i in range(0, len(parts), 2):
            url = parts[i]
            main_image_str = parts[i+1]
            is_main = 'true' in main_image_str
            parsed_images.append({'url': url, 'main_image': is_main})

    return parsed_images
