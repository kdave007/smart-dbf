from typing import Dict, List
from .base_identifier import BaseIdentifier
from .natural_key_identifier import NaturalKeyIdentifier
from .physical_position_identifier import PhysicalPositionIdentifier
from .hash_based_identifier import HashBasedIdentifier
from ..utils.sql_identifiers_manager import SQLIdentifiersManager

class IdentifierResolver:
    def __init__(self):
        """
        Initialize IdentifierResolver with SQLIdentifiersManager
        """
        self.sql_manager = SQLIdentifiersManager()

    def resolve_identifier_strategy(self, table_name: str) -> BaseIdentifier: 
        """
        Resolves the appropriate strategy based on table configuration from sql_identifiers.json
        
        Args:
            table_name: Name of the table to find configuration for
            
        Returns:
            BaseIdentifier instance based on table schema configuration
        """
        # Use SQLIdentifiersManager to get configuration
        schema_type = self.sql_manager.get_schema_type(table_name)
        
        if not schema_type:
            raise ValueError(f"No configuration found for table: {table_name}")
        
        # Get fields using the manager
        id_fields = self.sql_manager.get_id_fields(table_name)
        
        # Create the instances for their respective strategy
        if schema_type == "natural_key":
            return NaturalKeyIdentifier(table_name, {"key_fields": id_fields})
        
        elif schema_type == "physical_position":
            return PhysicalPositionIdentifier(table_name, {"key_fields": id_fields})
        
        elif schema_type == "composed_hash":
            hash_fields = self.sql_manager.get_hash_fields(table_name)
            return HashBasedIdentifier(table_name, {
                "key_fields": id_fields,
                "hash_fields": hash_fields
            })
        
        else:
            raise ValueError(f"Unknown schema type: {schema_type} for table: {table_name}")