from .base_identifier import BaseIdentifier
from typing import Dict, Any
import hashlib

class HashBasedIdentifier(BaseIdentifier):
    """Para tablas con PACK o sin identificadores estables"""
    
    def __init__(self, table_name: str, config: Dict):
        """
        Initialize with table name and configuration
        
        Args:
            table_name: Name of the table
            config: Configuration dictionary containing key_fields and hash_fields
        """
        super().__init__(table_name)
        self.config = config
    
    def calculate_identifier(self, record: Dict) -> Any:
        """
        Calcula hash MD5 de campos específicos definidos en config
        """
        hash_fields = self.config.get('hash_fields', [])
        values_to_hash = []
        
        for field in hash_fields:
            # Get field value, convert to string, and trim whitespace
            field_value = str(record.get(field, '')).strip()
            values_to_hash.append(field_value)
        
        # Join values with a separator and generate MD5 hash
        combined_string = '|'.join(values_to_hash)
        md5_hash = hashlib.md5(combined_string.encode('utf-8')).hexdigest()
        
        return f"HASH_{md5_hash}"

    def get_strategy_name(self) -> str:
        return "composed_hash"