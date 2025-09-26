from src.dbf_enc_reader.core import DBFReader
from pathlib import Path
import json
from typing import Dict, List, Any, Optional
from datetime import datetime
from src.dbf_enc_reader.converters import DataConverter
from src.filters import FilterManager

class Simple:
    def __init__(self, data_source: str, encryption_password: str, mapping_file_path: str = None, dll_path: str = None, filters_file_path: str = None, encrypted: bool = False):
        """
        Initialize Simple DBF controller
        
        Args:
            data_source: Path to the DBF file/directory
            encryption_password: Password for encrypted DBF
            mapping_file_path: Path to mappings.json file (optional)
            dll_path: Path to Advantage.Data.Provider.dll (optional)
            filters_file_path: Path to table_filters.json file (optional)
            encrypted: Whether the DBF files are encrypted (optional)
        """
        # Initialize DLL if path provided
        from src.dbf_enc_reader.connection import DBFConnection
        if dll_path:
            DBFConnection.set_dll_path(dll_path)
        
        self.data_source = data_source
        self.encryption_password = encryption_password
        self.encrypted = encrypted
        self.mapping_file_path = mapping_file_path or "src/utils/mappings.json"
        # Handle exe-compatible default path
        if filters_file_path is None:
            import sys
            if getattr(sys, 'frozen', False):
                # Running as exe
                base_path = Path(sys.executable).parent
                self.filters_file_path = str(base_path / "src" / "utils" / "rules.json")
            else:
                # Running in development
                self.filters_file_path = "src/utils/rules.json"
        else:
            self.filters_file_path = filters_file_path
        self.mappings = self._load_mappings()
        self.filter_manager = FilterManager(self.filters_file_path)
        self.connection = DBFConnection(data_source, encryption_password, encrypted)
        self.converter = DataConverter()
    
    def _load_mappings(self) -> Dict[str, Any]:
        """Load field mappings from JSON file"""
        try:
            with open(self.mapping_file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            print(f"Warning: Mapping file not found at {self.mapping_file_path}")
            return {}
        except json.JSONDecodeError as e:
            print(f"Error parsing mapping file: {e}")
            return {}
    
    def read_dbf_table(self, table_name: str, limit: Optional[int] = None, filters: Optional[List[Dict[str, Any]]] = None) -> List[Dict[str, Any]]:
        """
        Simple method to read DBF table data
        
        Args:
            table_name: Name of the table to read
            limit: Optional limit on number of records
            filters: Optional list of filter conditions
            
        Returns:
            List of records as dictionaries
        """
        reader = DBFReader(self.data_source, self.encryption_password, self.encrypted)
        return reader.read_table(table_name, limit, filters)
    
    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """
        Get information about table structure
        
        Args:
            table_name: Name of the table
            
        Returns:
            Dictionary containing table metadata
        """
        reader = DBFReader(self.data_source, self.encryption_password, self.encrypted)
        return reader.get_table_info(table_name)
    
    def get_table_data(self, table_name: str, limit: Optional[int] = None, date_range: Optional[Dict[str, str]] = None, value_filters: Optional[Dict[str, str]] = None) -> List[Dict[str, Any]]:
        """
        Get table data with optional filtering based on rules configuration
        
        Args:
            table_name: Name of the table to read
            limit: Optional limit on number of records
            date_range: Optional date range filter with 'from' and 'to' keys
            value_filters: Optional value filters dict with field names as keys
            
        Returns:
            List of records as dictionaries
        """
        filters = self.filter_manager.build_filters(table_name, date_range, value_filters)
        return self.read_dbf_table(table_name, limit, filters)
    
    # Filter-related methods now delegated to FilterManager
    def get_filter_config(self, table_name: str, filter_type: str = "date") -> Dict[str, Any]:
        """Get filter configuration for a specific table"""
        return self.filter_manager.get_filter_config(table_name, filter_type)
    
    def is_filter_enabled(self, table_name: str, filter_type: str) -> bool:
        """Check if a specific filter is enabled for a table"""
        return self.filter_manager.is_filter_enabled(table_name, filter_type)
    
    def get_available_filters(self, table_name: str) -> List[str]:
        """Get list of available filter types for a table"""
        return self.filter_manager.get_available_filters(table_name)
    

