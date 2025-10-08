import sys
from pathlib import Path
import time
from datetime import datetime

# Add the project root directory to the Python path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

from src.controllers.dbf_data import DBFData
from src.utils.logging_controller import LoggingController
from src.filters.filter_manager import FilterManager
from src.utils.config_manager import ConfigManager
from src.controllers.sql_references import SQLReferences
from src.controllers.data_comparator import DataComparator
from src.controllers.operation import Operation

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
    date_range={"from": "2025-09-25", "to": "2025-09-25"}
    
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

    print(f" {sql_records}")

    # print_sql_references(sql_records, 30)
    
    comparator = DataComparator(table)
    operations_obj = comparator.compare_records(
        dbf_records, 
        sql_records
    )

    print(f"New: {len(operations_obj['new'])}")
    print(f"Changed: {len(operations_obj['changed'])}")
    print(f"Unchanged: {len(operations_obj['unchanged'])}") 
    print(f"Deleted: {len(operations_obj['deleted'])}")

    # Get schema and field_id for API operations
    from src.utils.sql_identifiers_manager import SQLIdentifiersManager
    from src.utils.data_tables_schemas_manager import DataTablesSchemasManager
    
    sql_id_manager = SQLIdentifiersManager()
    data_table_schema_manager = DataTablesSchemasManager()
    
    schema_type = sql_id_manager.get_schema_type(table)
    field_id = data_table_schema_manager.get_id_field_name(schema_type)
    
    # Initialize Operation class with API base URL
    api_base_url = "https://api.example.com/v1"  # Replace with your actual API URL
    operation = Operation(api_base_url, table, simulate_response=True)
    
    # Send operations to API endpoints
    print("\n" + "="*50)
    print("SENDING OPERATIONS TO API")
    print("="*50)
    print(f"Schema: {schema_type}, Field ID: {field_id}")
    
    # Send new records
    if operations_obj['new']:
        print(f"Sending {len(operations_obj['new'])} new records...")
        new_response = operation.send_new_records(operations_obj['new'], schema_type, field_id)
        print(f"New records response: {new_response}")
    
    # Send updated records  
    if operations_obj['changed']:
        print(f"Sending {len(operations_obj['changed'])} updated records...")
        update_response = operation.send_updates(operations_obj['changed'], schema_type, field_id)
        print(f"Update response: {update_response}")
    
    # Send deleted records
    if operations_obj['deleted']:
        print(f"Sending {len(operations_obj['deleted'])} deleted records...")
        delete_response = operation.send_deletes(operations_obj['deleted'], schema_type, field_id)
        print(f"Delete response: {delete_response}")
    
    print(f"Unchanged records ({len(operations_obj['unchanged'])}) - no action needed")

    


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
        sql_records_list = list(sql_records)
        last_n_sql_records = sql_records_list[-last_N:] if len(sql_records_list) > last_N else sql_records_list
        
        print(f"\nShowing last {len(last_n_sql_records)} out of {len(sql_records_list)} SQL records:")
        print("-" * 50)
        
        for i, sql_record in enumerate(last_n_sql_records, 1):
            print(f"[{i}/{len(last_n_sql_records)}] SQL Record: {sql_record}")
    else:
       print("\nNo SQL records found in database")


if __name__ == "__main__":
    test()