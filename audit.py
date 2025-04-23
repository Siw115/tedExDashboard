import json
import logging
from datetime import datetime
import pandas as pd

def log_audit_event(cursor, user_id, action, table_name, record_id, old_values=None, new_values=None):
    logging.info(f"Audit log event: {action} on {table_name} (Record ID: {record_id}) by {user_id}.")

    def convert_timestamps_and_nan(obj):
        if isinstance(obj, dict):
            result = {}
            for k, v in obj.items():
                if isinstance(v, (pd.Timestamp, datetime)):
                    result[k] = v.isoformat()
                elif isinstance(v, (list, pd.Series)):
                    result[k] = [None if pd.isna(item) else item for item in v]
                else:
                    result[k] = None if pd.isna(v) else v
            return result
        return obj

    old_values_serializable = convert_timestamps_and_nan(old_values) if old_values else None
    new_values_serializable = convert_timestamps_and_nan(new_values) if new_values else None

    try:
        cursor.execute(
            """
            INSERT INTO audit_logs (user_id, action, table_name, record_id, old_values, new_values, action_time)
            VALUES (%s, %s, %s, %s, %s, %s, NOW())
            """,
            (
                user_id,
                action,
                table_name,
                record_id,
                json.dumps(old_values_serializable),
                json.dumps(new_values_serializable)
            )
        )
    except Exception as e:
        logging.error(f"Error logging audit event for {record_id}: {e}")
        cursor.connection.rollback()
