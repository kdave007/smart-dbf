from src.utils.strategy_selector import StrategySelector
from src.utils.config_manager import ConfigManager
import logging
from pathlib import Path

class DBFData:
    def __init__(self, data_source, venue_file_name, encrypted=False, encryption_password=None) -> None:
        self.data_source = data_source
        self.encrypted = encrypted
        self.encryption_password = encryption_password
        self.strategy_selector = StrategySelector()
        self.venue_file_name = venue_file_name
        
        # Initialize configuration
        self.config_manager = ConfigManager("ENV")
        self._load_config()
        
    def _load_config(self):
        """Load configuration from .env and venue.json files"""
        try:
            # Get combined configuration from .env and venue.json
            self.config = self.config_manager.get_combined_config(self.venue_file_name)
            
            # Set configuration parameters using ConfigManager methods
            self.dll_path = self.config_manager.get_dll_path()
            self.mapping_file_path = self.config_manager.get_mapping_file_path()
            self.filters_file_path = self.config_manager.get_filters_file_path()
            
            # Log loaded configuration
            logging.info(f"DBFData initialized with dll_path: {self.dll_path}")
            logging.info(f"Mapping file path: {self.mapping_file_path}")
            logging.info(f"Filters file path: {self.filters_file_path}")
            
        except Exception as e:
            logging.error(f"Error loading configuration: {str(e)}")
            raise

    def get(self, table_name, date_range):
        """Get data for specified table"""
        try:
            strategy = self.strategy_selector.get(table_name)
            logging.info(f"Getting data for table: {table_name} using strategy: {strategy}")
            
            if strategy=="simple":
                return self._simple(table_name, date_range)

            elif strategy=="composed":
                return self._composed(table_name, date_range)

            else:
                logging.error("dbf data :: invalid strategy")

           
        except Exception as e:
            logging.error(f"Error getting data for table {table_name}: {str(e)}")
            raise

    def _set_params(self, table_name):
        """Set parameters for specific table processing"""
        try:
            strategy = self.strategy_selector.get(table_name)
            # TODO: Implement parameter setting logic based on strategy
            print(f"Setting params for table: {table_name} with strategy: {strategy}")
        except Exception as e:
            logging.error(f"dbf data :: Error setting params for table {table_name}: {str(e)}")
            raise
    
    def get_config_value(self, key: str, default=None):
        """Get a specific configuration value"""
        return self.config_manager.get_config_value(key, default)
    
    def get_venue_info(self):
        """Get venue-specific information"""
        return self.config_manager.get_venue_info()

    def _simple(self, table_name, date_range):
        from src.tables_schemas.simple import Simple
        controller = Simple(
            data_source=self.data_source,
            encryption_password=self.encryption_password,
            mapping_file_path=self.mapping_file_path,
            dll_path=self.dll_path,
            filters_file_path=self.filters_file_path,
            encrypted=self.encrypted
        )

        data = controller.get_table_data(
            table_name = table_name,
            date_range=date_range,
            limit=None
        )

        return data

    
    def _composed(self, table_name, date_range):
        from src.tables_schemas.composed import Composed 
        controller = Composed(
            data_source=self.data_source,
            encryption_password=self.encryption_password,
            mapping_file_path=self.mapping_file_path,
            dll_path=self.dll_path,
            filters_file_path=self.filters_file_path,
            encrypted=self.encrypted
        )

        data = controller.get_table_data(
            table_name = table_name,
            date_range=date_range,
            limit=None
        )

        return data
        