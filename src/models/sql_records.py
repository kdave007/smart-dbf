

import json
import os
import sys
from typing import Optional
from src.db.sqlite_pool import SQLiteConnectionPool

class SQLRecords:
    def __init__(self) -> None:
        # Simple database configuration
        self.db_name = "dbf_test"  # From sql_identifiers.json
        self.db_path = "C:\\Users\\campo\\Documents\\projects\\smart-dbf\\dbf_test.db"  # From venue.json + db_name
        
        # Initialize SQLite connection pool
        self.db_pool = SQLiteConnectionPool(self.db_path, pool_size=5)
    
    def batch_select_by_id(self, records, id_field_name, table_name, version, batch_size):
        """Fetch matching records in safe batches for SQLite"""
        if not records:
            print("No records to process - returning empty results")
            return {}

        all_results = {}
        id_values = [record['__meta'][id_field_name] for record in records if '__meta' in record and id_field_name in record['__meta']]
        
        print(f"Processing {len(id_values)} IDs in batches of {batch_size}")

        # print(id_values)
        
        for i in range(0, len(id_values), batch_size):
            batch_ids = id_values[i:i + batch_size]
            # print(f"Batch {i//batch_size + 1}: {len(batch_ids)} records")
            
            placeholders = ','.join(['?' for _ in batch_ids])
            query = f"""
            SELECT {id_field_name}, hash_comparador FROM {table_name} 
            WHERE {id_field_name} IN ({placeholders})
            AND batch_version = ? AND eliminado = 0 
            """
            # print(f" query  {query}")
            
            # Extract version ID if it's a dictionary
            version_id = version.get('id') if isinstance(version, dict) else version
            params = tuple(batch_ids + [version_id])
            batch_results = self.db_pool.execute_query(query, params)
           
            # Convert sqlite3.Row objects to dict and build results
            for row in batch_results:
                row_dict = dict(row)  # Convert sqlite3.Row to dict
                all_results[row_dict[id_field_name]] = row_dict
        
        # print(f"Total matched: {len(all_results)} records")
        return all_results
    
    def select_all_records(self, table_name, limit=100):
        """Select all records from a table with optional limit"""
        query = f"""
        SELECT * FROM {table_name} 
        WHERE eliminado = 0 
        ORDER BY id_local DESC 
        LIMIT ?
        """
        
        params = (limit,)
        results = self.db_pool.execute_query(query, params)
        
        # Convert sqlite3.Row objects to dict
        all_records = []
        for row in results:
            row_dict = dict(row)
            all_records.append(row_dict)
        
        print(f"Retrieved {len(all_records)} records from {table_name}")
        return all_records


    