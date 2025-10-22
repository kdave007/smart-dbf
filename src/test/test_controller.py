import sys
from pathlib import Path
import time
from datetime import datetime

# Add the project root directory to the Python path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

from src.controllers.dbf_data import DBFData
from src.utils.logging_controller import logging
from src.filters.filter_manager import FilterManager
from src.utils.config_manager import ConfigManager
from src.controllers.sql_references import SQLReferences
from src.controllers.data_comparator import DataComparator

from src.controllers.sql_tracking_response import SQLTrackingResponse
from src.utils.date_calculator import DateCalculator
from src.utils.validate_location_client import ValidateLocationClient

from src.controllers.operation import Operation
# from src.controllers.operation_raw_og import Operation
import logging
# Initialize logging

def test(table):

    config_manager = ConfigManager.initialize("ENV")
    
    
    # Now you can access config anywhere
    data_source = config_manager.get_data_source()
    venue_file_name = config_manager.venue_file_name
    client_id = config_manager.config.get('plaza') + "_" + config_manager.config.get('sucursal')
    sql_enabled = config_manager.get_sql_enabled()
    debug_mode = config_manager.get_debug_mode()
    stop_flag = config_manager.get_stop_flag()
    
    # Check if STOP_FLAG is enabled
    if stop_flag: 
        logging.warning(f"STOP_FLAG ENABLED - Process will not execute")
        logging.warning(f"Set STOP_FLAG=0 in .env to enable processing")
        sys.exit()

    controller = DBFData(
        data_source=data_source,
        venue_file_name=venue_file_name,
        encrypted=False,
        encryption_password="X"
    )

    logging.info(f"                /////////////////////////////////////////////////////////////////////////")
    logging.info(f"                                 [ {table} :: {client_id} ]")
    logging.info(f"                /////////////////////////////////////////////////////////////////////////")

    logging.info(f"SQL enabled: {sql_enabled}")
    logging.info(f"Debug mode: {debug_mode}")

   
    """ date format YYYY-MM-DD (API format) """
    # Get date range from .env file
    date_calc = DateCalculator()
    date_range = date_calc.get_date_range_from_env(output_format="api")
    logging.info(f"Date range from .env: {date_range}")
    
    dbf_records = controller.get(table, date_range)
    logging.info(f"total dbf_records fetched for {table} : {len(dbf_records)}")

    # sys.exit()

    # Get field name from rules.json based on table name
    filter_manager = FilterManager(controller.filters_file_path)
    
    # Get the field configuration for this table
    table_rules = filter_manager.rules.get(table, {})
    
    # Get the identifier field for this table
    field_name = table_rules.get('identifier_field')


    validate_loc_params = table_rules.get('validate_location')
    reference = config_manager.config.get('sucursal')

    if dbf_records and reference and validate_loc_params:
        from src.utils.validate_location_client import ValidateLocationClient
        validate_client = ValidateLocationClient()
        valid_result = validate_client.check(reference, dbf_records, validate_loc_params.get('field_name'), validate_loc_params.get('exceptions'))

        if valid_result.get('error'):
            logging.error(f"THIS DBF CLIENT DOES NOT CORRESPOND TO THE SET LOCATION :: Sucursal = {reference} - DBF = { valid_result.get('client_found')}")
            return {
                "new": None,
                "updated": None,
                "deleted": None,
                "unchanged": None
            }

    
    # Get configuration values (reuse the same config_manager from above)
    
    print_dbf_records(dbf_records, field_name, 30)
    
    sql_references_manager = SQLReferences(table)
    sql_records = sql_references_manager._get_by_batches(dbf_records, date_range.get('from'), date_range.get('to'))
    # print(f" {sql_records}")
    # print_sql_references(sql_records, 30)
    
    comparator = DataComparator(table)
    operations_obj = comparator.compare_records(
        dbf_records, 
        sql_records
    )

    logging.info(f"      Table status operations {table}: ")
    logging.info(f"------- New: {len(operations_obj['new'])}")
    logging.info(f"------- update: {len(operations_obj['changed'])}")
    logging.info(f"------- Unchanged: {len(operations_obj['unchanged'])}") 
    logging.info(f"------- Deleted: {len(operations_obj['deleted'])}")

    

    # Get schema and field_id for API operations
    from src.utils.sql_identifiers_manager import SQLIdentifiersManager
    from src.utils.data_tables_schemas_manager import DataTablesSchemasManager
    
    sql_id_manager = SQLIdentifiersManager()
    data_table_schema_manager = DataTablesSchemasManager()
    
    schema_type = sql_id_manager.get_schema_type(table)
    field_id = data_table_schema_manager.get_id_field_name(schema_type)
    version = sql_id_manager.get_batch_version()
    
    # Send operations to API endpoints
    print("\n" + "="*50)
    print("SENDING OPERATIONS TO API")
    print("="*50)
    print(f"Schema: {schema_type}, Field ID: {field_id}")


    from src.controllers.batch_processor import BatchProcessor

    batch_pro = BatchProcessor(table, config_manager, sql_enabled)
    results = batch_pro.process_all_operations(operations_obj, schema_type, field_id, version)
    
    logging.info(f" Table {table} processing completed")
    
    """ TODO right now the process only sends the json records and expects an ok status with
        an array of the records processed statuses and queu id, and sets the sql db status to 2
        and when we query the sql references, we avoid status 2 cause its been processing by the server
        this is only for testing, we still miss the GET request to actually update the real status of 
        each record in the sqlite db
    """
    
    return results

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


def setup_logging():
    base_dir = Path.cwd()  
    log_dir = base_dir / "logs"
    log_dir.mkdir(exist_ok=True)

    current_date_log = datetime.now().strftime("%d_%m_%Y")  
    log_file = log_dir / f"SDBF_{current_date_log}.log"
    
    # Configurar logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_file, encoding='utf-8')
        ],
        force=True  # ⚠️ IMPORTANTE: Sobrescribe configuración existente
    )
    
    logging.info(f"Logs saved in : {log_file}")


if __name__ == "__main__":

    setup_logging()

    table_1="XCORTE"
    table_2="CANOTA"
    table_3="CUNOTA"

    

    print("main")
    border = "*" * 80
    spacing = "*" + " " * 78 + "*"
    message = "*" + " " * 25 + "STARTING Smart DBF v1.13 " + " " * 25 + "*"


    logging.info(border)
    logging.info(spacing)
    logging.info(message)
    logging.info(spacing)
    logging.info(border)

    
    # Process all tables and collect results
    all_results = {}
    
    all_results[table_1] = test(table_1)
    all_results[table_2] = test(table_2)
    all_results[table_3] = test(table_3)
    
    # Log aggregated results for all tables
    logging.info(f" ")
    logging.info("="*80)
    logging.info("FINAL PROCESSING RESULTS SUMMARY - ALL TABLES")
    logging.info("="*80)
    
    for table_name, results in all_results.items():
        logging.info(f"/// TABLE: {table_name} ///")
        
        if results['new']:
            logging.info(f"  NEW:")
            logging.info(f"    - Total records: {results['new']['total_records']}")
            logging.info(f"    - Batches: {results['new']['successful_batches']}/{results['new']['total_batches']} successful")
            logging.info(f"    - Success rate: {results['new']['success_rate']}%")
            logging.info(f" ")
        
        if results['updated']:
            logging.info(f"  UPDATED:")
            logging.info(f"    - Total records: {results['updated']['total_records']}")
            logging.info(f"    - Batches: {results['updated']['successful_batches']}/{results['updated']['total_batches']} successful")
            logging.info(f"    - Success rate: {results['updated']['success_rate']}%")
            logging.info(" ")
        
        if results['deleted']:

            logging.info(f"  DELETED:")

            if table_name == 'CANOTA':
                logging.warning("**** Deleted CANOTA records (headers) will automatically mark matching records (partitions) in CUNOTA as deleted")

            logging.info(f"    - Total records: {results['deleted']['total_records']}")
            logging.info(f"    - Batches: {results['deleted']['successful_batches']}/{results['deleted']['total_batches']} successful")
            logging.info(f"    - Success rate: {results['deleted']['success_rate']}%")
            logging.info(" ")
        
        if results['unchanged']:
            logging.info(f"  UNCHANGED: {results['unchanged']} records")
            logging.info(" ")
    
    logging.info("="*80)
    logging.info(" ALL TABLES PROCESSED ")
    logging.info("="*80)
