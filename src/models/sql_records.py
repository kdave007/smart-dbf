"""
SQL Records class
"""

import json
import os
from typing import Optional
from src.db.sqlite_pool import SQLiteConnectionPool

class SQLRecords:
    def __init__(self) -> None:
        # Simple database configuration
        self.db_name = "dbf_test"  # From sql_identifiers.json
        self.db_path = "C:\\Users\\campo\\Documents\\projects\\smart-dbf\\dbf_test.db"  # From venue.json + db_name
        
        # Initialize SQLite connection pool
        self.db_pool = SQLiteConnectionPool(self.db_path, pool_size=5)

    