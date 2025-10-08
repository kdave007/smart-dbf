from .base_identifier import BaseIdentifier
from typing import Dict, Any
from ..utils.data_tables_schemas_manager import DataTablesSchemasManager

class NaturalKeyIdentifier(BaseIdentifier):
    """Para tablas con folios únicos (cabeceras, catálogos)"""
    
    def __init__(self, table_name: str, config: Dict):
        """
        Initialize with table name and configuration
        
        Args:
            table_name: Name of the table
            config: Configuration dictionary containing key_fields
        """
        super().__init__(table_name)
        self.config = config
        self.schema_type = "natural_key"

    def calculate_identifier(self, record: Dict) -> Any:
        """
        Usa uno o múltiples campos como clave natural
        """
        
        key_fields = self.config.get('key_fields')

        print(f' key fields :: {key_fields}')

        if len(key_fields) == 1:
            # Clave simple: 'folio' -> valor
            return record.get(key_fields[0])
        else:
            # Clave compuesta: ['folio', 's sucursal'] -> 'folio_s sucursal'
            parts = [str(record.get(field, '')) for field in key_fields]
            return '_'.join(parts)
    
    def get_strategy_name(self) -> str:
        return self.schema_type 

    def get_sql_field_id_name(self) -> str:
        """
            Returns the strategy name in the sql table
        """
        return DataTablesSchemasManager().get_id_field_name(self.schema_type)