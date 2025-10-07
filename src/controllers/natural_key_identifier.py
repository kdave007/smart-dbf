from .base_identifier import BaseIdentifier
from typing import Dict, Any

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
        return "natural_key"