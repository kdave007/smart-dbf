import json
import os
from typing import Dict, List
from .base_identifier import BaseIdentifier
from .natural_key_identifier import NaturalKeyIdentifier
from .physical_position_identifier import PhysicalPositionIdentifier
from .hash_based_identifier import HashBasedIdentifier

class IdentifierResolver:
    def __init__(self, config_path: str = None):
        """
        Initialize IdentifierResolver with configuration from sql_identifiers.json
        
        Args:
            config_path: Path to sql_identifiers.json file. If None, uses default path.
        """
        if config_path is None:
            # Default path relative to this file
            current_dir = os.path.dirname(os.path.abspath(__file__))
            config_path = os.path.join(current_dir, "..", "utils", "sql_identifiers.json")
        
        self.config_path = config_path
        self.config = self._load_config()

    def _load_config(self) -> Dict:
        """
        Load configuration from sql_identifiers.json file
        
        Returns:
            Configuration dictionary
        """
        try:
            with open(self.config_path, 'r', encoding='utf-8') as file:
                return json.load(file)
        except FileNotFoundError:
            raise FileNotFoundError(f"Configuration file not found: {self.config_path}")
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in configuration file: {e}")

    def resolve_identifier_strategy(self, table_name: str) -> BaseIdentifier: 
        """
        Resolves the appropriate strategy based on table configuration from sql_identifiers.json
        
        Args:
            table_name: Name of the table to find configuration for
          
            
        Returns:
            BaseIdentifier instance based on table schema configuration
        """
        # Find table configuration in the tables array
        table_config = None
        for table in self.config.get("tables", []):
            if table.get("name") == table_name:
                table_config = table
                break
        
        if not table_config:
            raise ValueError(f"No configuration found for table: {table_name}")
        
        # Get schema type and fields from the JSON configuration
        schema_type = table_config.get("schema")
        
        id_fields = table_config.get("id_fields", [])

        if schema_type == "composed_hash":
            hash_fields = table_config.get("hash_fields", [])


        #now create the instances for their respective strategy
        if schema_type == "natural_key":
            return NaturalKeyIdentifier(table_name, {"key_fields": id_fields})
        
        elif schema_type == "physical_position":
            return PhysicalPositionIdentifier(table_name, {"key_fields": id_fields})
        
        elif schema_type == "composed_hash":
            return HashBasedIdentifier(table_name, {
                "key_fields": id_fields,
                "hash_fields": hash_fields
            })
        
        else:
            raise ValueError(f"Unknown schema type: {schema_type} for table: {table_name}")