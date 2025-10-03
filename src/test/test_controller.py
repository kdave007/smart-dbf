import sys
from pathlib import Path
import time
from datetime import datetime

# Add the project root directory to the Python path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))



from src.controllers.dbf_data import DBFData
from src.utils.logging_controller import LoggingController

# Initialize logging
logging = LoggingController.get_instance()

def test():
    print("main")
    border = "*" * 80
    spacing = "*" + " " * 78 + "*"
    message = "*" + " " * 25 + "STARTING Smart DBF v1.0 " + " " * 25 + "*"

    logging.info(border)
    logging.info(spacing)
    logging.info(message)
    logging.info(spacing)
    logging.info(border)

    # Get data source from config manager
    from src.filters.filter_manager import FilterManager
    from src.utils.config_manager import ConfigManager
    config_manager = ConfigManager("ENV")
    config = config_manager.get_combined_config("venue.json")
    data_source = config.get('data_source')

    # data_source = r"C:\Users\campo\Documents\dbf_encriptados\pospcp"
    
    controller = DBFData(
        data_source=data_source,
        encrypted=False,
        encryption_password="X3WGTXG5QJZ6K9ZC4VO2"
    )

    table="CUNOTA"
    date_range={"from": "2025-01-22", "to": "2025-01-22"}
    
    result = controller.get(table, date_range)
    print(f"results {len(result)}")

    # Get field name from rules.json based on table name
  
    filter_manager = FilterManager(controller.filters_file_path)
    
    # Get the field configuration for this table
    table_rules = filter_manager.rules.get(table, {})
    
    # Get the identifier field for this table
    field_name = table_rules.get('identifier_field', 'NO_REFEREN')
    
    for record in result:
        print(f"{field_name} : {record.get(field_name)}  index : {record.get('__meta')}")


if __name__ == "__main__":
    test()