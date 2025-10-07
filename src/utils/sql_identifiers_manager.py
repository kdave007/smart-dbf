"""
SQL Identifiers Manager

This class is responsible for managing the configuration of SQL identifiers
"""
import json
import os
from typing import Dict, List, Optional

class SQLIdentifiersManager:
    def __init__(self) -> None:
        self.config_path = self._get_config_path()
        self.config = self._load_config()
    
    def _get_config_path(self) -> str:
        """Get the path to sql_identifiers.json file"""
        current_dir = os.path.dirname(os.path.abspath(__file__))
        return os.path.join(current_dir, "sql_identifiers.json")
    
    def _load_config(self) -> Dict:
        """Load configuration from sql_identifiers.json file"""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as file:
                return json.load(file)
        except FileNotFoundError:
            raise FileNotFoundError(f"Configuration file not found: {self.config_path}")
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in configuration file: {e}")
    
    def get_table_config(self, table_name: str) -> Optional[Dict]:
        """
        Get configuration for a specific table
        
        Args:
            table_name: Name of the table to find configuration for
            
        Returns:
            Table configuration dictionary or None if not found
        """
        for table in self.config.get("tables", []):
            if table.get("name") == table_name:
                return table
        return None
    
    def get_schema_type(self, table_name: str) -> Optional[str]:
        """
        Get schema type for a table
        
        Args:
            table_name: Name of the table
            
        Returns:
            Schema type ('natural_key', 'physical_position', 'composed_hash') or None
        """
        table_config = self.get_table_config(table_name)
        if table_config:
            return table_config.get("schema") or table_config.get("type")
        return None
    
    def get_id_fields(self, table_name: str) -> List[str]:
        """
        Get ID fields for a table
        
        Args:
            table_name: Name of the table
            
        Returns:
            List of ID field names
        """
        table_config = self.get_table_config(table_name)
        if table_config:
            return table_config.get("id_fields", [])
        return []
    
    def get_hash_fields(self, table_name: str) -> List[str]:
        """
        Get hash fields for a table (only for composed_hash schema)
        
        Args:
            table_name: Name of the table
            
        Returns:
            List of hash field names
        """
        table_config = self.get_table_config(table_name)
        if table_config:
            return table_config.get("hash_fields", [])
        return []
    
    def get_database_name(self) -> Optional[str]:
        """
        Get database name from configuration
        
        Returns:
            Database name or None
        """
        return self.config.get("db", {}).get("name")
    
    def get_actions(self) -> Dict:
        """
        Get actions configuration
        
        Returns:
            Actions dictionary
        """
        return self.config.get("actions", {})
    
    def get_additional_columns(self, table_name: str) -> List[Dict]:
        """
        Get additional columns for a table
        
        Args:
            table_name: Name of the table
            
        Returns:
            List of additional column definitions
        """
        table_config = self.get_table_config(table_name)
        if table_config:
            return table_config.get("additional_columns", [])
        return []
    
    def get_additional_columns_names_only(self, table_name: str) -> List[str]:
        """
        Get additional column names only for a table
        
        Args:
            table_name: Name of the table
            
        Returns:
            List of additional column names
        """
        additional_columns = self.get_additional_columns(table_name)
        return [col.get("name") for col in additional_columns if col.get("name")]
    
    def get_skip_columns_names(self, table_name: str) -> List[str]:
        """
        Get skip columns for a table
        
        Args:
            table_name: Name of the table
            
        Returns:
            List of column names to skip
        """
        table_config = self.get_table_config(table_name)
        if table_config:
            return table_config.get("skip_columns", [])
        return []
    
    def get_batch_version(self) -> Dict:
        """
        Get batch version configuration
        
        Returns:
            Batch version dictionary with comment and id
        """
        return self.config.get("batch_version", {})