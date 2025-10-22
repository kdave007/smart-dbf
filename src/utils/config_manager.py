import os
import sys
import json
from pathlib import Path
from typing import Dict, Any
import logging

class ConfigManager:
    _instance = None
    
    @staticmethod
    def _get_base_path():
        """Get base path for exe or development environment - ALWAYS EXTERNAL"""
        if getattr(sys, 'frozen', False):
            # Running as exe - use exe directory (external)
            exe_dir = Path(sys.executable).parent
            logging.info(f"Running as EXE - Base path: {exe_dir}")
            return exe_dir
        else:
            # Running in development - use project root
            dev_path = Path(__file__).parent.parent.parent
            logging.info(f"Running in development - Base path: {dev_path}")
            return dev_path
    
    @classmethod
    def get_instance(cls):
        """Get singleton instance of ConfigManager"""
        if cls._instance is None:
            raise RuntimeError("ConfigManager not initialized. Call initialize() first.")
        return cls._instance
    
    @classmethod
    def initialize(cls, source, venue_file_name=None):
        """Initialize the singleton instance with source and optional venue file"""
        if cls._instance is None:
            # If no venue_file_name provided, try to read from .env
            if venue_file_name is None:
                venue_file_name = cls._get_venue_file_from_env()
            cls._instance = cls(source, venue_file_name)
        return cls._instance
    
    @staticmethod
    def _get_venue_file_from_env():
        """Read VENUE_FILE_NAME from .env file - EXTERNAL ONLY"""
        base_path = ConfigManager._get_base_path()
        env_path = base_path / '.env'
        
        if not env_path.exists():
            logging.error(f".env file not found at {env_path}")
            raise FileNotFoundError(f".env file not found at {env_path}")
        
        try:
            with open(env_path, 'r', encoding='utf-8') as file:
                for line in file:
                    line = line.strip()
                    
                    # Skip empty lines and comments
                    if not line or line.startswith('#'):
                        continue
                    
                    # Parse key=value pairs
                    if '=' in line:
                        key, value = line.split('=', 1)
                        key = key.strip()
                        value = value.strip().strip('"')  # Remove quotes if present
                        
                        if key == 'VENUE_FILE_NAME':
                            logging.info(f"Venue file configured: {value}")
                            return value
            
            raise ValueError("VENUE_FILE_NAME not found in .env file")
        except Exception as e:
            raise Exception(f"Error reading VENUE_FILE_NAME from .env: {str(e)}")
    
    def __init__(self, source, venue_file_name=None):
        self.source = source  # API or ENV
        self.config = {}
        self.venue_file_name = venue_file_name
        
        logging.info(f"ConfigManager initialized - Source: {source}, Venue: {venue_file_name}")
        
        # Load configuration on initialization
        if source.upper() == "ENV":
            self._from_env()
        elif source.upper() == "API":
            self._from_api()
        
        # Load venue file if provided
        if venue_file_name:
            venue_config = self.load_json_config(venue_file_name)
            self.config.update(venue_config)
        
    def get_params(self) -> Dict[str, Any]:
        if self.source == "API":
            return self._from_api()
        else:
            return self._from_env()

    def _from_env(self) -> Dict[str, Any]:
        """Load configuration parameters from .env file - EXTERNAL ONLY"""
        base_path = self._get_base_path()
        env_path = base_path / '.env'
        
        if not env_path.exists():
            logging.error(f".env file not found at {env_path}")
            raise FileNotFoundError(f".env file not found at {env_path}")
        
        config = {}
        
        try:
            with open(env_path, 'r', encoding='utf-8') as file:
                for line_num, line in enumerate(file, 1):
                    line = line.strip()
                    
                    # Skip empty lines and comments
                    if not line or line.startswith('#'):
                        continue
                    
                    # Parse key=value pairs
                    if '=' in line:
                        key, value = line.split('=', 1)
                        key = key.strip()
                        value = value.strip()
                        
                        # Convert numeric values
                        if value.isdigit():
                            config[key] = int(value)
                        elif value.replace('.', '', 1).isdigit():
                            config[key] = float(value)
                        else:
                            config[key] = value
                    else:
                        print(f"Warning: Invalid line format at line {line_num}: {line}")
        
        except Exception as e:
            logging.error(f"config manager :: Invalid line format at line {line_num}: {line}")
            raise Exception(f"Error reading .env file: {str(e)}")
        
        self.config = config
        logging.info(f"Loaded {len(config)} configuration values from .env")
        return config
    
    def _from_api(self) -> Dict[str, Any]:
        """Load configuration parameters from API (to be implemented)"""
        # TODO: Implement API configuration loading
        pass
    
    def get_config_value(self, key: str, default=None):
        """Get a specific configuration value"""
        return self.config.get(key, default)
    
    def load_json_config(self, json_file_name: str) -> Dict[str, Any]:
        """Load configuration from JSON file in project root - EXTERNAL ONLY"""
        base_path = self._get_base_path()
        json_path = base_path / json_file_name
        
        if not json_path.exists():
            logging.error(f"JSON config file not found at {json_path}")
            raise FileNotFoundError(f"JSON config file not found at {json_path}")
        
        try:
            with open(json_path, 'r', encoding='utf-8') as file:
                json_config = json.load(file)
                logging.info(f"Loaded venue configuration from {json_file_name}")
                return json_config
        except json.JSONDecodeError as e:
            logging.error(f"Invalid JSON format in {json_file_name}: {str(e)}")
            raise Exception(f"Invalid JSON format in {json_file_name}: {str(e)}")
        except Exception as e:
            logging.error(f"Error reading JSON config file {json_file_name}: {str(e)}")
            raise Exception(f"Error reading JSON config file {json_file_name}: {str(e)}")
    
    def get_combined_config(self, json_file_name: str = None) -> Dict[str, Any]:
        """Get combined configuration from .env and optional JSON file"""
        combined_config = self.get_params().copy()
        
        if json_file_name:
            json_config = self.load_json_config(json_file_name)
            combined_config.update(json_config)
        
        return combined_config
    
    def get_mapping_file_path(self) -> str:
        """Get mapping file path - INTERNAL in exe"""
        if getattr(sys, 'frozen', False):
            # En producción, el archivo está dentro del EXE
            base_path = Path(sys._MEIPASS) if hasattr(sys, '_MEIPASS') else Path(sys.executable).parent
            mapping_file = base_path / "src/utils/mappings.json"
        else:
            # En desarrollo, busca en utils
            utils_dir = Path(__file__).parent
            mapping_file = utils_dir / "mappings.json"
        return str(mapping_file) if mapping_file.exists() else None
    
    def get_filters_file_path(self) -> str:
        """Get filters file path - INTERNAL in exe"""
        if getattr(sys, 'frozen', False):
            # En producción, el archivo está dentro del EXE
            base_path = Path(sys._MEIPASS) if hasattr(sys, '_MEIPASS') else Path(sys.executable).parent
            filters_file = base_path / "src/utils/rules.json"
        else:
            # En desarrollo, busca en utils
            utils_dir = Path(__file__).parent
            filters_file = utils_dir / "rules.json"
        return str(filters_file) if filters_file.exists() else None
    
    def get_venue_info(self) -> Dict[str, str]:
        """Get venue-specific information from configuration"""
        return {
            'sucursal': self.config.get('sucursal'),
            'plaza': self.config.get('plaza'),
            'config_endpoint': self.config.get('config_endpoint')
        }
    
    def get_data_source(self) -> str:
        """Get data source path from configuration"""
        return self.config.get('data_source')
    
    def get_stop_flag(self) -> int:
        """Get stop flag from environment"""
        return int(self.config.get('STOP_FLAG', 0))
    
    def get_debug_mode(self) -> bool:
        """Get debug mode from environment"""
        return bool(int(self.config.get('DEBUG', 0)))
    
    def get_dbf_chunks_size(self) -> int:
        """Get DBF chunks size from environment"""
        return int(self.config.get('DBF_CHUNKS_SIZE', 500))
    
    def get_sql_enabled(self) -> bool:
        """Get SQL enabled flag from environment"""
        return bool(int(self.config.get('SQL_ENABLED', 0)))
    
    def get_sqlite_path(self) -> str:
        """Get SQLite database directory path from configuration"""
        return self.config.get('sqlite')
    
    def get_db_name(self) -> str:
        """Get database name from .env file"""
        db_name = self.config.get('DB_NAME')
        
        if not db_name:
            logging.error("DB_NAME not found in configuration")
            raise ValueError("DB_NAME not found in .env file")
        
        return db_name
    
    def get_full_db_path(self) -> str:
        """Get full SQLite database path (directory + db_name.db)"""
        sqlite_dir = self.get_sqlite_path()
        db_name = self.get_db_name()
        
        if not sqlite_dir:
            raise ValueError("SQLite path not configured")
        
        full_path = Path(sqlite_dir) / f"{db_name}.db"
        return str(full_path)
    
    def get_identifier_field(self, table_name: str) -> str:
        """Get identifier field for a table from rules.json"""
        rules_file_path = self.get_filters_file_path()
        
        if not rules_file_path:
            logging.error("rules.json file not found")
            return None
        
        try:
            with open(rules_file_path, 'r', encoding='utf-8') as f:
                rules_data = json.load(f)
                table_rules = rules_data.get(table_name, {})
                identifier_field = table_rules.get('identifier_field')
                return identifier_field
        except Exception as e:
            logging.error(f"Error loading identifier field from rules.json: {e}")
            return None