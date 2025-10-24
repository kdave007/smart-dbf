import sys
from typing import Optional
from src.db.sqlite_pool import SQLiteConnectionPool
from src.utils.config_manager import ConfigManager
import logging
from datetime import datetime

class ServerPendingSQL:
    def __init__(self) -> None:
         # Get database configuration from ConfigManager singleton
        config_manager = ConfigManager.get_instance()
        self.db_name = config_manager.get_db_name()
        self.db_path = config_manager.get_full_db_path()
        
        # Initialize SQLite connection pool
        self.db_pool = SQLiteConnectionPool(self.db_path, pool_size=5)

    def select_by_update_date_queued(self, field_id, table_name, version, start_date, end_date):
        """ may be i can fetch data with insertado and ultima_revision at instead of """
        try:
            start = self._format_date_to_iso(start_date)
            end = self._format_date_to_iso(end_date)

            query = f"""
                SELECT {field_id}, id_cola FROM {table_name} 
                WHERE status = 'BATCH_QUEUED'
                AND eliminado = 0
                AND batch_version = ?
                AND DATE(ultima_revision) BETWEEN ? AND ?;
            """
            print(f"query {query}")

            params = (version, start, end)
            results = self.db_pool.execute_query(query, params)

            all_records = []
            for row in results:
                row_dict = dict(row)
                all_records.append(row_dict)
            
            print(f"Retrieved {len(all_records)} records from {table_name}")
            return all_records

        except Exception as e:
            logging.error(f"[ServerPendingSQL] Error in select_by_update_date: {e}")
            return None
        
        
    
    def _format_date_to_iso(self, date_value):
        """
        Parse date value and format to ISO date format (YYYY-MM-DD).
        Handles various input formats including datetime objects and strings.
        
        Args:
            date_value: Date value (can be datetime, string, etc.)
            
        Returns:
            Date string in YYYY-MM-DD format or None if parsing fails
        """
        if date_value is None:
            return None
        
        try:
            # If it's already a datetime object
            if hasattr(date_value, 'year') and hasattr(date_value, 'month') and hasattr(date_value, 'day'):
                return f"{date_value.year:04d}-{date_value.month:02d}-{date_value.day:02d}"
            
            # If it's a string, try common formats
            if isinstance(date_value, str):
                date_value = date_value.strip()
                
                # Try common date formats
                formats = [
                    '%d/%m/%Y',  # 19/10/2025
                    '%m/%d/%Y',  # 10/19/2025
                    '%Y-%m-%d',  # 2025-10-19 (already ISO)
                    '%d-%m-%Y',  # 19-10-2025
                    '%Y/%m/%d',  # 2025/10/19
                ]
                
                for fmt in formats:
                    try:
                        parsed_date = datetime.strptime(date_value, fmt)
                        return f"{parsed_date.year:04d}-{parsed_date.month:02d}-{parsed_date.day:02d}"
                    except ValueError:
                        continue
            
            # If we can't parse it, log warning and return None
            logging.warning(f"[SQLRecords] Could not parse date value: {date_value}")
            return None
            
        except Exception as e:
            logging.warning(f"[SQLRecords] Error formatting date {date_value}: {e}")
            return None