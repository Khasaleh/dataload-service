import csv
from typing import List, Dict, Any
from sqlalchemy.orm import Session
from pydantic import BaseModel, ValidationError

from app.db.models import ProductOrm, CategoryOrm
from app.dataload.models.meta_tags_csv import MetaTagCsvRow, MetaTypeEnum


class DataloadErrorDetail(BaseModel):
    row_number: int
    raw_data: Dict[str, Any]
    error_type: str # e.g., "Validation", "NotFound", "Database", "FileAccess", "FileProcessing"
    error_message: str

class DataloadSummary(BaseModel):
    total_rows_processed: int = 0
    successful_updates: int = 0
    validation_errors: int = 0
    target_not_found_errors: int = 0
    database_errors: int = 0
    error_details: List[DataloadErrorDetail] = []


def load_meta_tags_from_csv(db: Session, csv_file_path: str) -> DataloadSummary:
    summary = DataloadSummary()

    try:
        with open(csv_file_path, mode='r', encoding='utf-8-sig') as csvfile: # utf-8-sig handles potential BOM
            reader = csv.DictReader(csvfile) # Let DictReader handle the header line directly

            for i, raw_row_data in enumerate(reader):
                summary.total_rows_processed += 1
                current_row_number = i + 2  # CSV row number (1-indexed data, after header)

                # Clean keys from the raw_row_data dictionary produced by DictReader
                cleaned_row_input = {
                    key.strip().lower().replace(' ', '_') if key else '_unknown_empty_header_': value
                    for key, value in raw_row_data.items()
                }
                # Filter out any entry that might have resulted from an empty header
                cleaned_row_input = {k: v for k, v in cleaned_row_input.items() if k != '_unknown_empty_header_'}

                try:
                    validated_row = MetaTagCsvRow(**cleaned_row_input)
                except ValidationError as e:
                    summary.validation_errors += 1
                    summary.error_details.append(DataloadErrorDetail(
                        row_number=current_row_number,
                        raw_data=raw_row_data, # Log original row
                        error_type="Validation",
                        error_message=str(e)
                    ))
                    db.rollback() # Ensure no partial transaction from previous iteration if error occurs before commit
                    continue

                try:
                    target_updated_in_db = False
                    if validated_row.meta_type == MetaTypeEnum.PRODUCT:
                        product = db.query(ProductOrm).filter(
                            ProductOrm.name == validated_row.target_identifier,
                            ProductOrm.business_details_id == validated_row.business_details_id
                        ).first()

                        if product:
                            update_applied = False
                            if validated_row.meta_title is not None and product.seo_title != validated_row.meta_title:
                                product.seo_title = validated_row.meta_title
                                update_applied = True
                            if validated_row.meta_description is not None and product.seo_description != validated_row.meta_description:
                                product.seo_description = validated_row.meta_description
                                update_applied = True
                            if validated_row.meta_keywords is not None and product.keywords != validated_row.meta_keywords:
                                product.keywords = validated_row.meta_keywords
                                # print(f"DEBUG: Row {current_row_number} | Immediately after assignment, product.keywords: {repr(product.keywords)}") # REMOVED
                                update_applied = True

                            if update_applied:
                                target_updated_in_db = True
                        else:
                            summary.target_not_found_errors += 1
                            summary.error_details.append(DataloadErrorDetail(
                                row_number=current_row_number,
                                raw_data=raw_row_data,
                                error_type="NotFound",
                                error_message=(
                                    f"PRODUCT with name '{validated_row.target_identifier}' and "
                                    f"business_details_id '{validated_row.business_details_id}' not found."
                                )
                            ))
                            db.rollback() # Rollback before continuing
                            continue

                    elif validated_row.meta_type == MetaTypeEnum.CATEGORY:
                        category = db.query(CategoryOrm).filter(
                            CategoryOrm.name == validated_row.target_identifier
                        ).first()

                        if category:
                            update_applied = False
                            # print(f"DEBUG_CAT: Row {current_row_number} | Validated Title: {repr(validated_row.meta_title)}, Category SEO Title: {repr(category.seo_title)}") # REMOVED
                            if validated_row.meta_title is not None and category.seo_title != validated_row.meta_title:
                                category.seo_title = validated_row.meta_title
                                # print(f"DEBUG: Row {current_row_number} | Immediately after assignment, category.seo_title: {repr(category.seo_title)}") # REMOVED
                                update_applied = True
                            # print(f"DEBUG_CAT: Row {current_row_number} | Validated Desc: {repr(validated_row.meta_description)}, Category SEO Desc: {repr(category.seo_description)}") # REMOVED
                            if validated_row.meta_description is not None and category.seo_description != validated_row.meta_description:
                                category.seo_description = validated_row.meta_description
                                # print(f"DEBUG: Row {current_row_number} | Immediately after assignment, category.seo_description: {repr(category.seo_description)}") # REMOVED
                                update_applied = True
                            # print(f"DEBUG_CAT: Row {current_row_number} | Validated Keywords: {repr(validated_row.meta_keywords)}, Category SEO Keywords: {repr(category.seo_keywords)}") # REMOVED
                            if validated_row.meta_keywords is not None and category.seo_keywords != validated_row.meta_keywords:
                                category.seo_keywords = validated_row.meta_keywords
                                # print(f"DEBUG: Row {current_row_number} | Immediately after assignment, category.seo_keywords: {repr(category.seo_keywords)}") # REMOVED
                                update_applied = True

                            if update_applied:
                                target_updated_in_db = True
                        else:
                            summary.target_not_found_errors += 1
                            summary.error_details.append(DataloadErrorDetail(
                                row_number=current_row_number,
                                raw_data=raw_row_data,
                                error_type="NotFound",
                                error_message=f"CATEGORY with name '{validated_row.target_identifier}' not found."
                            ))
                            db.rollback() # Rollback before continuing
                            continue

                    if target_updated_in_db:
                        db.commit()
                        summary.successful_updates += 1
                    # If no update was applied (e.g. data was same or all optional fields were None), no commit needed, not counted as success.

                except Exception as e:
                    db.rollback()
                    summary.database_errors += 1
                    summary.error_details.append(DataloadErrorDetail(
                        row_number=current_row_number,
                        raw_data=raw_row_data,
                        error_type="Database",
                        error_message=f"An unexpected error occurred during database operation: {str(e)}"
                    ))

    except FileNotFoundError:
        summary.error_details.append(DataloadErrorDetail(
            row_number=0,
            raw_data={},
            error_type="FileAccess",
            error_message=f"CSV file not found at path: {csv_file_path}"
        ))
    except Exception as e:
        summary.error_details.append(DataloadErrorDetail(
            row_number=0,
            raw_data={},
            error_type="FileProcessing",
            error_message=f"An unexpected error occurred during CSV file processing: {str(e)}"
        ))

    return summary
