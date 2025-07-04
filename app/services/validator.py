import re
from pydantic import ValidationError
from app.utils.redis_utils import get_from_id_map
from collections import defaultdict
from app.models.schemas import (
    BrandCsvModel, AttributeCsvModel, ReturnPolicyCsvModel,
    ProductItemModel, ProductPriceModel, MetaTagModel,
    ProductCsvModel, ErrorDetailModel, ErrorType,
    CategoryCsvModel
)
from typing import List, Dict, Optional
from app.utils.redis_utils import get_from_id_map

MODEL_MAP = {
    "brands":           BrandCsvModel,
    "attributes":       AttributeCsvModel,
    "return_policies":  ReturnPolicyCsvModel,
    "product_items":    ProductItemModel,
    "product_prices":   ProductPriceModel,
    "meta_tags":        MetaTagModel,
    "products":         ProductCsvModel,
    "categories":       CategoryCsvModel
}


def generate_slug(input_string: str) -> str:
    slug = input_string.lower().strip()
    slug = slug.replace(' ', '-')
    slug = re.sub(r'[^a-z0-9-]', '', slug)
    return slug.strip('-')


def check_category_hierarchy(
    records: List[Dict],
    session_id: str
) -> List[Dict]:
    """
    Ensure no sub‐category is added under an existing product‐holding category,
    and that every parent in the hierarchy is either already in the DB or
    is present in this batch of CSV records.
    """
    errors: List[Dict] = []
    # all the new paths in this upload
    seen_paths = {rec["category_path"] for rec in records}

    for idx, rec in enumerate(records, start=1):
        path = rec.get("category_path", "")
        segments = [seg for seg in path.split("/") if seg]
        # for each prefix (excluding the full path), check:
        for level in range(1, len(segments)):
            parent = "/".join(segments[:level])

            # 1) disallow if parent already has products in DB
            if get_from_id_map(session_id, "products", parent):
                errors.append({
                    "row": idx,
                    "field": "category_path",
                    "error": (
                        f"Cannot create '{path}' under '{parent}': "
                        "existing products found in that category."
                    ),
                    "value": path
                })
                # no need to check further up for this row
                break

            # 2) disallow if parent is neither in DB nor in this CSV
            if not get_from_id_map(session_id, "categories", parent) \
               and parent not in seen_paths:
                errors.append({
                    "row": idx,
                    "field": "category_path",
                    "error": f"Parent category '{parent}' missing for '{path}'",
                    "value": path
                })
                break

    return errors

def validate_csv(load_type: str, records: List[Dict], session_id: str) -> (List[Dict], List[Dict]):
    errors: List[Dict] = []
    valid_rows: List[Dict] = []

    Model = MODEL_MAP.get(load_type)
    if not Model:
        return [{"row": None, "field": None, "error": f"Unsupported load type: {load_type}"}], []

    for i, row in enumerate(records):
        try:
            inst = Model(**row)
            data = inst.dict()
            # for categories: set defaults
            if load_type == 'categories':
                # default enabled
                data['enabled'] = bool(data.get('enabled', True))
                # active default
                act = str(data.get('active', '')).strip().upper()
                data['active'] = 'ACTIVE' if act=='ACTIVE' else 'INACTIVE'
                # generate url if missing
                if not data.get('url'):
                    data['url'] = generate_slug(data.get('name',''))
            valid_rows.append(data)
        except ValidationError as e:
            for err in e.errors():
                errors.append({
                    "row": i + 1,
                    "field": ".".join(str(f) for f in err['loc']),
                    "error": err['msg']
                })
    # Category-specific business rules
    if load_type == 'categories' and valid_rows:
        cat_errs = check_category_hierarchy(valid_rows, session_id)
        errors.extend(cat_errs)

    return errors, valid_rows


def check_file_uniqueness(records: List[Dict], unique_key: str) -> List[Dict]:
    errors = []
    key_counts = defaultdict(list)
    for i, record in enumerate(records):
        key_value = record.get(unique_key)
        if key_value is not None:
            key_counts[key_value].append(i+1)
    for key, rows in key_counts.items():
        if len(rows) > 1:
            errors.append({
                'error': 'Duplicate key',
                'field': unique_key,
                'key': key,
                'rows': rows
            })
    return errors


def check_referential_integrity(
    records: List[Dict],
    field_to_check: str,
    referenced_entity_type: str,
    session_id: str
) -> List[Dict]:
    errors = []
    for i, record in enumerate(records):
        val = record.get(field_to_check)
        if val and not get_from_id_map(session_id, referenced_entity_type, val):
            errors.append({
                'row': i+1,
                'field': field_to_check,
                'error': f"Referenced {referenced_entity_type} not found",
                'value': val
            })
    return errors
