from numbers import Number
from ..dbf_enc_reader.mapping_manager import MappingManager
from pathlib import Path
import hashlib
import json
from ..tables_schemas.simple import Simple

class Composed:
    def __init__(self, data_source: str, encryption_password: str, mapping_file_path: str = None, dll_path: str = None, filters_file_path: str = None, encrypted: bool = False):
        self.table= None
        self.matching_field = None
        self.simple_controller = Simple(data_source, encryption_password, mapping_file_path, dll_path, filters_file_path, encrypted)


    def _get_references(self, table, date_range, limit):
        references = self.simple_controller.get_table_data(table,limit,date_range)
        print(f' total references {len(references)}')
        print(f' REF {references[0]}')
        return references
    
    def _get_composed_parent_fields(self, table_name: str):
        """Get composed_parent profile fields from field_map.json"""
        try:
            with open("src/utils/field_map.json", 'r', encoding='utf-8') as f:
                data = json.load(f)
                tables = data.get('tables', {})
                table_key = table_name.replace('.DBF', '')
                table_config = tables.get(table_key, {})
                profiles = table_config.get('profiles', {})
                composed_parent = profiles.get('composed_parent', {})
                fields = composed_parent.get('fields', [])
                print(f"[Composed] Found composed_parent fields for {table_name}: {fields}")
                return fields
        except Exception as e:
            print(f"[Composed] Error loading field_map.json: {e}")
            return []

    def get_table_data(self, date_range, limit: int = 300):
        """
            TODO here i need to fetch data only by the reference matching field and then search in the actual target table 
        """
        self._get_composed_parent_fields(table)
        return self._get_references(date_range=date_range, limit=limit)
