# app/utils/date_utils.py
from datetime import datetime, timezone

class ServerDateTime:
    @staticmethod
    def now_epoch_ms() -> int:
        """
        Return current UTC time as milliseconds since epoch.
        """
        return int(datetime.now(tz=timezone.utc).timestamp() * 1000)

    @staticmethod
    def now_epoch_s() -> int:
        """
        Return current UTC time as seconds since epoch.
        """
        return int(datetime.now(tz=timezone.utc).timestamp())

# ------------------------------------------------------
# Then, in app/services/db_loaders.py (inside load_brand_to_db):

# At top of file, import:
# from app.utils.date_utils import ServerDateTime

# ... inside load_brand_to_db, before building new_brands_mappings:

# Determine server timestamps and normalize 'active'
for record in records_data:
    # business_details_id already passed in as business_details_id
    record['business_details_id'] = business_details_id
    # Replace any CSV-provided fields with server-generated ones
    record['created_by'] = user_id  # <-- pass current user's ID into this loader
    record['created_date'] = ServerDateTime.now_epoch_ms()
    record['updated_by'] = user_id
    record['updated_date'] = ServerDateTime.now_epoch_ms()
    # Normalize active field
    active_flag = record.get('active', '').strip().upper()
    record['active'] = 'ACTIVE' if active_flag in ('TRUE', 'ACTIVE') else 'INACTIVE'

# Then when you do bulk_insert_mappings, these fields will populate the table correctly.
