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
from src.controllers.sql_tracking_response import SQLTrackingResponse
from src.utils.date_calculator import DateCalculator

# Initialize logging
logging = LoggingController.get_instance()

def test(table):
    

    # Initialize ConfigManager singleton - venue file name read from .env
    config_manager = ConfigManager.initialize("ENV")
    
    # Now you can access config anywhere
    data_source = config_manager.get_data_source()
    venue_file_name = config_manager.venue_file_name

    # data_source = r"C:\Users\campo\Documents\dbf_encriptados\pospcp"
    
    controller = DBFData(
        data_source=data_source,
        venue_file_name=venue_file_name,
        encrypted=False,
        encryption_password="X3WGTXG5QJZ6K9ZC4VO2"
    )

   
    

    """ date format YYYY-MM-DD (API format) """
    # Get date range from .env file
    date_calc = DateCalculator()
    date_range = date_calc.get_date_range_from_env(output_format="api")
    logging.info(f"Date range from .env: {date_range}")

    # sys.exit()
    
    dbf_records = controller.get(table, date_range)
    print(f"dbf_recordss {len(dbf_records)}")

    # Get field name from rules.json based on table name
    filter_manager = FilterManager(controller.filters_file_path)
    
    # Get the field configuration for this table
    table_rules = filter_manager.rules.get(table, {})
    
    # Get the identifier field for this table
    field_name = table_rules.get('identifier_field')
    
    # Get configuration values (reuse the same config_manager from above)
    sql_enabled = config_manager.get_sql_enabled()
    debug_mode = config_manager.get_debug_mode()

    print(f"SQL enabled: {sql_enabled}")
    print(f"Debug mode: {debug_mode}")
    
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
    version = sql_id_manager.get_batch_version()
    
    # Initialize Operation class with API base URL
    client_id = config_manager.config.get('plaza') + "_" + config_manager.config.get('sucursal')
    
    operation = Operation(config_manager.config, table, client_id, simulate_response=debug_mode)
    
    # Initialize SQL Tracking Response
    tracking = SQLTrackingResponse(table)
    
    # Send operations to API endpoints
    print("\n" + "="*50)
    print("SENDING OPERATIONS TO API")
    print("="*50)
    print(f"Schema: {schema_type}, Field ID: {field_id}")
    
    # Send new records
    if operations_obj['new']:
        print(f"Sending {len(operations_obj['new'])} new records...")
        new_response = operation.send_new_records(operations_obj['new'], schema_type, field_id, version)
        print(f"New records response: {new_response}")
        
        # Track the response
        if sql_enabled:
            tracking.post_insert_records_status(new_response, operations_obj['new'], field_id, version)
    
    #Send updated records  
    if operations_obj['changed']:
        print(f"Sending {len(operations_obj['changed'])} updated records...")
        update_response = operation.send_updates(operations_obj['changed'], schema_type, field_id)
        print(f"Update response: {update_response}")
        
        #Track the response
        if sql_enabled:
            tracking.post_update_records_status(update_response, operations_obj['changed'], field_id, version)
    
    # Send deleted records
    # if operations_obj['deleted']:
    #     print(f"Sending {len(operations_obj['deleted'])} deleted records...")
    #     delete_response = operation.send_deletes(operations_obj['deleted'], schema_type, field_id)
    #     print(f"Delete response: {delete_response}")
        
        # Track the response
        # if sql_enabled:
        #     tracking.post_delete_records_status(delete_response, operations_obj['deleted'], field_id, version)
    
    print(f"Unchanged records ({len(operations_obj['unchanged'])}) - no action needed")


    """ TODO right now the process only sends the json records and expects an ok status with
        an array of the records processed statuses and queu id, and sets the sql db status to 2
        and when we query the sql references, we avoid status 2 cause its been processing by the server
        this is only for testing, we still miss the GET request to actually update the real status of 
        each record in the sqlite db
    """

    


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
    table_1="XCORTE"
    table_2="CANOTA"
    table_3= "CUNOTA"

    print("main")
    border = "*" * 80
    spacing = "*" + " " * 78 + "*"
    message = "*" + " " * 25 + "STARTING Smart DBF v1.0 " + " " * 25 + "*"

    logging.info(border)
    logging.info(spacing)
    logging.info(message)
    logging.info(spacing)
    logging.info(border)

    # print(f"                ******** Processing {table_1} ******")
    # test(table_1)

    print(f"                ******** Processing {table_2} ******")
    test(table_2)
    
    # print(f"                ******** Processing {table_3} ******")
    # test(table_3)
