from .base_identifier import BaseIdentifier
from typing import Dict, Any
import sys

class PhysicalPositionIdentifier(BaseIdentifier):
    """Para tablas con duplicados + sin PACK (partidas)"""
    
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
        Usa RECNO (posición física) como identificador
        """
        # key_fields = [field.lower() for field in self.config.get('key_fields', [])]

        # print(f' key fields :: {key_fields}')

        # sys.exit(1)

        # if len(key_fields) == 1:
        #     # Clave simple: 'folio' -> valor
        return record.get('__meta').get('recno')



    def get_strategy_name(self) -> str:
        return "physical_position"