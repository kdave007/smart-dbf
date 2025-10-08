import sys
from pathlib import Path
import time
from datetime import datetime

# Add the project root directory to the Python path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

from src.controllers import data_comparator
from src.controllers.dbf_data import DBFData
from src.utils.logging_controller import LoggingController
from src.filters.filter_manager import FilterManager
from src.utils.config_manager import ConfigManager
from src.controllers.sql_references import SQLReferences
from src.controllers.data_comparator import DataComparator

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
   
    config_manager = ConfigManager("ENV")
    config = config_manager.get_combined_config("venue.json")
    data_source = config.get('data_source')

    # data_source = r"C:\Users\campo\Documents\dbf_encriptados\pospcp"
    
    controller = DBFData(
        data_source=data_source,
        encrypted=False,
        encryption_password="X3WGTXG5QJZ6K9ZC4VO2"
    )

    table="XCORTE"
    # table="CANOTA"
    table="CUNOTA"
    """ date format YYYY MM DD """
    # date_range={"from": "2025-01-22", "to": "2025-01-22"}
    date_range={"from": "2025-08-29", "to": "2025-08-31"}
    
    dbf_records = controller.get(table, date_range)
    print(f"dbf_recordss {len(dbf_records)}")

    # Get field name from rules.json based on table name
    filter_manager = FilterManager(controller.filters_file_path)
    
    # Get the field configuration for this table
    table_rules = filter_manager.rules.get(table, {})
    
    # Get the identifier field for this table
    field_name = table_rules.get('identifier_field')
    
    print_dbf_records(dbf_records, field_name, 30)
    
    sql_references_manager = SQLReferences(table)
    sql_records = sql_references_manager._get_by_batches(dbf_records)

    # print_sql_references(sql_records, 30)
    

    # data_comparator = DataComparator()
    # data_comparator.get_blueprint(table, dbf_records)

def print_dbf_records(dbf_records, field_name, last_N):
    total_records = len(dbf_records)
    last_n_records = dbf_records[-last_N:] if len(dbf_records) > last_N else dbf_records

    print(f"\nShowing last {len(last_n_records)} out of {total_records} DBF records:")
    print("-" * 50)
    
    for i, record in enumerate(last_n_records, 1):
        print(f"[{i}/{len(last_n_records)}] {field_name}: {record.get(field_name)}  index: {record.get('__meta')}")

def print_sql_references(sql_records, last_N):
    if len(sql_records) > 0:
        print(f"\nSQL records found: {len(sql_records)}")
        
        # Show last n SQL records
        sql_records_list = list(sql_records.values())
        last_n_sql_records = sql_records_list[-last_N:] if len(sql_records_list) > last_N else sql_records_list
        
        print(f"\nShowing last {len(last_n_sql_records)} out of {len(sql_records_list)} SQL records:")
        print("-" * 50)
        
        for i, sql_record in enumerate(last_n_sql_records, 1):
            print(f"[{i}/{len(last_n_sql_records)}] SQL Record: {sql_record}")
    else:
       print("\nNo SQL records found in database")


if __name__ == "__main__":
    test()