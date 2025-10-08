import json
import os
from typing import Dict, List, Optional

class DataTablesSchemasManager:
    def __init__(self) -> None:
        self.config_path = self._get_config_path()
        self.config = self._load_config()
    
    def _get_config_path(self) -> str:
        """Get the path to data_tables_schemas.json file"""
        current_dir = os.path.dirname(os.path.abspath(__file__))
        return os.path.join(current_dir, "data_tables_schemas.json")
    
    def _load_config(self) -> Dict:
        """Load configuration from data_tables_schemas.json file"""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as file:
                return json.load(file)
        except FileNotFoundError:
            raise FileNotFoundError(f"Configuration file not found: {self.config_path}")
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in configuration file: {e}")
    
    def get_schema_options(self) -> List[str]:
        """
        Get available schema options
        
        Returns:
            List of schema options ('natural_key', 'physical_position', 'composed_hash')
        """
        return self.config.get("schemas", {}).get("options", [])
    
    def get_schema_description(self) -> str:
        """
        Get schema description
        
        Returns:
            Description of schema types
        """
        return self.config.get("schemas", {}).get("description", "")
    
    def get_common_columns(self) -> List[Dict]:
        """
        Get common columns that apply to all schema types
        
        Returns:
            List of common column definitions
        """
        return self.config.get("schemas", {}).get("common", {}).get("columns", [])
    
    def get_common_columns_names_only(self) -> List[str]:
        """
        Get common column names only
        
        Returns:
            List of common column names
        """
        columns = self.config.get("schemas", {}).get("common", {}).get("columns", [])
        return [col.get("name") for col in columns if col.get("name")]
    
    def get_schema_columns(self, schema_type: str) -> List[Dict]:
        """
        Get columns specific to a schema type
        
        Args:
            schema_type: Schema type ('natural_key', 'physical_position', 'composed_hash')
            
        Returns:
            List of schema-specific column definitions
        """
        return self.config.get("schemas", {}).get(schema_type, {}).get("columns", [])
    
    def get_all_columns_for_schema(self, schema_type: str) -> List[Dict]:
        """
        Get all columns (common + schema-specific) for a schema type
        
        Args:
            schema_type: Schema type ('natural_key', 'physical_position', 'composed_hash')
            
        Returns:
            List of all column definitions for the schema
        """
        common_columns = self.get_common_columns()
        schema_columns = self.get_schema_columns(schema_type)
        return common_columns + schema_columns
    
    def get_column_names_for_schema(self, schema_type: str) -> List[str]:
        """
        Get all column names for a schema type
        
        Args:
            schema_type: Schema type ('natural_key', 'physical_position', 'composed_hash')
            
        Returns:
            List of column names
        """
        all_columns = self.get_all_columns_for_schema(schema_type)
        return [col.get("name") for col in all_columns if col.get("name")]

    def get_id_field_name(self, schema_type):
     
        
        return self.config.get("schemas", {}).get(schema_type, {}).get("id_column_name")