
from .identifier_resolver import IdentifierResolver
from ..utils.sql_identifiers_manager import SQLIdentifiersManager
from ..utils.data_tables_schemas_manager import DataTablesSchemasManager

class DataComparator:
    def __init__(self, table_name) -> None:
        self.table_name= table_name
  
        self.sql_id_manager = SQLIdentifiersManager()
        self.data_table_schema_manager = DataTablesSchemasManager()


    def compare_records(self, dbf_records, sqlite_records):
        """Main method - compares two lists of records"""

        schema_type = self.sql_id_manager.get_schema_type(self.table_name)
        id_field_name = self.data_table_schema_manager.get_id_field_name(schema_type)

        # Step 1: Convert both lists to dictionaries for fast lookup
        dbf_dict = self._list_to_dict(dbf_records, id_field_name)
        # sqlite_dict = self._list_to_dict(sqlite_records, id_field_name)
        
        # Step 2: Find differences
        new_records = self._find_new(dbf_dict, sqlite_records)
        changed_records = self._find_changed(dbf_dict, sqlite_records)
        unchanged_records = self._find_unchanged(dbf_dict, sqlite_records)
        deleted_records = self._find_deleted(dbf_dict, sqlite_records)
        
        # Step 3: Return results
        return {
            'new': new_records,
            'changed': changed_records, 
            'unchanged': unchanged_records,
            'deleted': deleted_records
        }
    

    def _list_to_dict(self, records, id_field_name):
        """Convert list to dictionary - handles both DBF and SQLite structures"""
        result = {}
        for record in records:
            # Check if it's a DBF record (has __meta) or SQLite record
            if '__meta' in record:
                # DBF record: get ID from __meta
                record_id = record['__meta'][id_field_name]
            else:
                # SQLite record: get ID directly  
                record_id = record[id_field_name]
            result[record_id] = record
        return result
    
    def _find_new(self, dbf_dict, sqlite_dict):
        """Find records in DBF but not in SQLite"""
        new = []
        for record_id, record in dbf_dict.items():
            if record_id not in sqlite_dict:
                new.append(record)
        return new

    def _find_changed(self, dbf_dict, sqlite_dict):
        """Find records in both but with different hash"""
        changed = []
        for record_id, dbf_record in dbf_dict.items():
            if record_id in sqlite_dict:
                dbf_hash = dbf_record['__meta']['hash_comparador']
                sqlite_hash = sqlite_dict[record_id]['hash_comparador']
                if dbf_hash != sqlite_hash:
                    changed.append(dbf_record)
        return changed

    def _find_unchanged(self, dbf_dict, sqlite_dict):
        """Find records in both with same hash"""
        unchanged = []
        for record_id, dbf_record in dbf_dict.items():
            if record_id in sqlite_dict:
                dbf_hash = dbf_record['__meta']['hash_comparador']
                sqlite_hash = sqlite_dict[record_id]['hash_comparador']
                if dbf_hash == sqlite_hash:
                    unchanged.append(dbf_record)
        return unchanged

    def _find_deleted(self, dbf_dict, sqlite_dict):
        """Find records in SQLite but not in DBF"""
        deleted = []
        for record_id, sqlite_record in sqlite_dict.items():
            if record_id not in dbf_dict:
                deleted.append(sqlite_record)
        return deleted
        
        
        