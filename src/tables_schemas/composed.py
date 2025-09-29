from numbers import Number
from ..dbf_enc_reader.mapping_manager import MappingManager
from pathlib import Path
import hashlib
import json
from ..tables_schemas.simple import Simple

class Composed:
    def __init__(self, data_source: str, encryption_password: str, mapping_file_path: str = None, dll_path: str = None, filters_file_path: str = None, encrypted: bool = False):
        self.related_table_name = None
        self.matching_field = None
        self.simple_controller = Simple(data_source, encryption_password, mapping_file_path, dll_path, filters_file_path, encrypted)

    def _set_related_table(self, related_table_name: str = None, matching_field: str = None):
        self.related_table_name = related_table_name
        self.matching_field = matching_field

    def _get_references(self, date_range, limit):
        references = self.simple_controller.get_table_data(self.related_table_name,limit,date_range)
        print(f' total references {len(references)}')
        print(f' REF {references[0]}')
        return references
    
    def get_table_data(self, table_name, related_table, matching_field, date_range, limit: int = 300):
        """
            TODO here i need to fetch data only by the reference matching field and then search in the actual target table 
        """
        self._set_related_table(related_table, matching_field)
        return self._get_references(date_range=date_range, limit=limit)
