

import json
import os
import sys
from typing import Optional
from src.db.sqlite_pool import SQLiteConnectionPool
from src.utils.config_manager import ConfigManager
import logging
from datetime import datetime

class SQLRecords:
    def __init__(self) -> None:
        # Get database configuration from ConfigManager singleton
        config_manager = ConfigManager.get_instance()
        self.db_name = config_manager.get_db_name()
        self.db_path = config_manager.get_full_db_path()
        
        # Initialize SQLite connection pool
        self.db_pool = SQLiteConnectionPool(self.db_path, pool_size=5)
    
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
    
    def batch_select_by_id(self, records, id_field_name, table_name, version, batch_size):
        """Fetch matching records in safe batches for SQLite"""
        if not records:
            print("No records to process - returning empty results")
            return {}

        all_results = {}
        id_values = [record['__meta'][id_field_name] for record in records if '__meta' in record and id_field_name in record['__meta']]
        
        print(f"-- SQL Processing {len(id_values)} IDs in batches of {batch_size}")

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
        
        print(f"Total SQL references matched: {len(all_results)} records")
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

    def batch_select_by_id_and_date_range(self, records, id_field_name, table_name, version, start_date, end_date, batch_size):
        """Fetch matching records in safe batches for SQLite"""
        if not records:
            print("No records to process - returning empty results")
            return {}

        all_results = {}
        id_values = [record['__meta'][id_field_name] for record in records if '__meta' in record and id_field_name in record['__meta']]
        
        print(f"-- SQL Processing {len(id_values)} IDs in batches of {batch_size}")

        # print(id_values)
        
        for i in range(0, len(id_values), batch_size):
            batch_ids = id_values[i:i + batch_size]
            # print(f"Batch {i//batch_size + 1}: {len(batch_ids)} records")
            
            placeholders = ','.join(['?' for _ in batch_ids])
            query = f"""
                SELECT {id_field_name}, hash_comparador, fecha_original FROM {table_name} 
                WHERE ({id_field_name} IN ({placeholders}) OR fecha_original BETWEEN ? AND ?)
                AND batch_version = ? AND eliminado = 0 
            """
            # print(f" query  {query}")
            
            # Extract version ID if it's a dictionary
            version_id = version.get('id') if isinstance(version, dict) else version
            params = tuple(batch_ids + [start_date, end_date, version_id])
            batch_results = self.db_pool.execute_query(query, params)
           
            # Convert sqlite3.Row objects to dict and build results
            for row in batch_results:
                row_dict = dict(row)  # Convert sqlite3.Row to dict
                all_results[row_dict[id_field_name]] = row_dict
        
        print(f"Total SQL references matched: {len(all_results)} records")
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

    def insert_sent_data(self, table_name, records, field_id, version, delete_flag, waiting_id, status=1):
        """Generic batch insert with transaction support"""
        if not records:
            print("No records to insert")
            return False
        
        # Extract version ID if it's a dictionary
        version_id = version.get('id') if isinstance(version, dict) else version
        
        # Get a connection from the pool
        conn = self.db_pool.get_connection()
        
        try:
            # Start transaction
            conn.execute("BEGIN TRANSACTION")
            
            insert_count = 0
            for record in records:
                # Extract the field value from __meta
                id_field_value = record.get('__meta', {}).get(field_id)
                ref_date_raw = record.get('__meta', {}).get('ref_date', None)
                
                # Parse ref_date to ISO format (YYYY-MM-DD)
                ref_date = self._format_date_to_iso(ref_date_raw) if ref_date_raw else None
                
                if id_field_value is None:
                    print(f"Warning: No {field_id} found in record __meta")
                    continue
                
                # Generic insert query 
                query = f"""
                    INSERT INTO {table_name} (
                        {field_id}, 
                        batch_version, 
                        status, 
                        hash_comparador,
                        eliminado,
                        id_cola,
                        fecha_original
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """
                
                params = (id_field_value, version_id, status, record['__meta']['hash_comparador'], delete_flag, waiting_id, ref_date)
                conn.execute(query, params)
                insert_count += 1
            
            # Commit transaction
            conn.execute("COMMIT")
            logging.info(f"-- Successfully inserted {insert_count} records into {table_name} batch cola id: {waiting_id}")
            return True
            
        except Exception as e:
            # Rollback transaction on error
            try:
                conn.execute("ROLLBACK")
                logging.error(f"-- Transaction rolled back due to error: {str(e)}")
            except Exception as rollback_error:
                logging.error(f"-- Error during rollback: {str(rollback_error)}")
            return False
            
        finally:
            # Return connection to pool
            self.db_pool.return_connection(conn)

    def update_sent_data(self, table_name, records, field_id, version, waiting_id, status=1):
        """Generic batch insert with transaction support"""
        if not records:
            print("No records to insert")
            return False
        
        # Extract version ID if it's a dictionary
        version_id = version.get('id') if isinstance(version, dict) else version
        current_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        conn = self.db_pool.get_connection()
        try:
            # Start transaction
            conn.execute("BEGIN TRANSACTION")

            update_count = 0
            for record in records:
                # Extract the field value from __meta
                id_field_value = record.get('__meta', {}).get(field_id)
                if id_field_value is None:
                    print(f"Warning: No {field_id} found in record __meta")
                    continue
                
                # Generic update query 
                query = f"""
                    UPDATE {table_name} 
                    SET status = ?, 
                        hash_comparador = ?,
                        id_cola = ?,
                        ultima_revision = ?
                    WHERE {field_id} = ? AND batch_version = ?
                """

                # Orden correcto de parámetros:
                # status, hash_comparador, id_cola, id_field_value, version_id
                
                params = (status, record['__meta']['hash_comparador'], waiting_id, current_date, id_field_value, version_id)
                conn.execute(query, params)
                update_count += 1
            
            # Commit transaction
            conn.execute("COMMIT")
            logging.info(f"-- Successfully updated {update_count} records into {table_name} batch cola id: {waiting_id}")
            return True
                
        except Exception as e:
            # Rollback transaction on error
            try:
                conn.execute("ROLLBACK")
                logging.error(f"-- sql records Transaction rolled back due to error: {str(e)}")
            except Exception as rollback_error:
                logging.error(f"-- Error during rollback: {str(rollback_error)}")
            return False
            
        finally:
            # Return connection to pool
            self.db_pool.return_connection(conn)
        



    