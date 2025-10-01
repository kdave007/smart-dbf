import os
import json
from pathlib import Path
from typing import Dict, Any
from src.utils.logging_controller import logging

class ConfigManager:
    def __init__(self, source):
        self.source = source  #API or ENV
        self.config = {}
        
    def get_params(self) -> Dict[str, Any]:
        if self.source == "API":
            return self._from_api()
        else:
            return self._from_env()

    def _from_env(self) -> Dict[str, Any]:
        """Load configuration parameters from .env file"""
        env_path = Path(__file__).parent.parent.parent / '.env'
        
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
        return config
    
    def _from_api(self) -> Dict[str, Any]:
        """Load configuration parameters from API (to be implemented)"""
        # TODO: Implement API configuration loading
        pass
    
    def get_config_value(self, key: str, default=None):
        """Get a specific configuration value"""
        return self.config.get(key, default)
    
    def load_json_config(self, json_file_name: str) -> Dict[str, Any]:
        """Load configuration from JSON file in utils directory"""
        json_path = Path(__file__).parent.parent.parent / json_file_name
        
        if not json_path.exists():
            logging.error(f"JSON config file not found at {json_path}")
            raise FileNotFoundError(f"JSON config file not found at {json_path}")
        
        try:
            with open(json_path, 'r', encoding='utf-8') as file:
                json_config = json.load(file)
                logging.info(f"Loaded JSON config from {json_file_name}")
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
        """Get mapping file path from utils directory"""
        utils_dir = Path(__file__).parent
        mapping_file = utils_dir / "mappings.json"
        return str(mapping_file) if mapping_file.exists() else None
    
    def get_filters_file_path(self) -> str:
        """Get filters file path from utils directory"""
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
    