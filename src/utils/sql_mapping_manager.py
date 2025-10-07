
from .sql_identifiers_manager import SQLIdentifiersManager
from .data_tables_schemas_manager import DataTablesSchemasManager

class SQLMappingManager:
    
    def __init__(self):
        self.sql_identifiers_manager = SQLIdentifiersManager()
        self.schemas_manager = DataTablesSchemasManager()

    def get_table_mapping(self, table_name):
            columns = self._get_table_columns(table_name)

    def _get_strategy_name(self, table_name):
        """it will get the strategy name"""
        return self.sql_identifiers_manager.get_schema_type(table_name)
    

    def _get_table_columns(self, table_name):
        """gets only the table columns to fill, not the db auto-generated"""
        # Get the strategy/schema type first
        strategy_name = self._get_strategy_name(table_name)
        
        if not strategy_name:
            return []
        
        # Get additional columns from sql_identifiers.json (set by the specific table name)
        addition_table_column_names = self.sql_identifiers_manager.get_additional_columns_names_only(table_name)
        
        # Get skip columns from sql_identifiers.json (set by the specific table name) 
        skip_table_column_names = self.sql_identifiers_manager.get_skip_columns_names(table_name)
        
        # Get schema columns from data_tables_schemas.json (set by schema)
        schema_columns = self.schemas_manager.get_column_names_for_schema(strategy_name)
        
        # Get columns based on strategy
        # Combine additional columns and schema columns
        all_columns = addition_table_column_names + schema_columns
        
        # Remove skip columns from the final list (optimize for empty/small skip lists)
        if skip_table_column_names:
            # Convert to set for O(1) lookup performance
            skip_set = set(skip_table_column_names)
            final_columns = [col for col in all_columns if col not in skip_set]
        else:
            # No columns to skip, return all columns as-is
            final_columns = all_columns
        
        return final_columns
