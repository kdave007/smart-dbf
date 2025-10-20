import logging


class ValidateLocationClient:
    """
    Validates that records have the correct location/sucursal reference.
    
    Logic:
    - Check if the field_name in each record matches the reference_location
    - If exception=1 (True): Allow empty values, check next record if current is empty
    - If exception=0 (False): Field must match reference_location, no empty values allowed
    - After 2 failed attempts, let the record pass (don't block the entire process)
    """

    def check(self, reference_location, records, field_name, exception):
        """
        Validate location field in records against reference.
        
        Args:
            reference_location (str): Expected location value (e.g., sucursal name)
            records (list): List of DBF records to validate
            field_name (str): Name of the field to check (e.g., "TIENDA")
            exception (int): 1 = allow empty values, 0 = strict validation
        
        Returns:
            dict: {
                'error': bool,
                'client_found': str,
                'valid': bool,
                'message': str
            }
        """
        if not records:
            logging.warning("No records to validate")
            return {
                'error': False,
                'client_found': None,
                'valid': True,
                'message': 'No records to validate'
            }
        
        allow_empty = bool(exception)
        total_records = len(records)
        invalid_count = 0
        empty_count = 0
        valid_count = 0
        client_found = None  # Track the actual client value found
        
        logging.info(f" Validating location field '{field_name}' against reference: '{reference_location}'")
        logging.info(f"   Exception mode: {'ENABLED (empty allowed)' if allow_empty else 'DISABLED (strict)'}")
        
        # Check first record
        first_record = records[0]
        first_value = first_record.get(field_name, '').strip()
        if first_value:
            client_found = first_value
        
        if not first_value:
            empty_count += 1
            if allow_empty:
                logging.warning(f"  Record 1/{total_records}: Field '{field_name}' is EMPTY (allowed by exception)")
                
                # Try second record if available
                if len(records) > 1:
                    second_record = records[1]
                    second_value = second_record.get(field_name, '').strip()
                    
                    if not second_value:
                        empty_count += 1
                        logging.warning(f"  Record 2/{total_records}: Field '{field_name}' is EMPTY (allowed by exception)")
                        logging.warning(f"  First 2 records are empty, but continuing due to exception mode...")
                    elif second_value != reference_location:
                        invalid_count += 1
                        if second_value:
                            client_found = second_value
                        logging.error(f" Record 2/{total_records}: Field '{field_name}' = '{second_value}' (expected: '{reference_location}')")
                        logging.warning(f"  Validation failed on second attempt, but continuing...")
                    else:
                        valid_count += 1
                        client_found = second_value
                        logging.info(f" Record 2/{total_records}: Field '{field_name}' = '{second_value}' (VALID)")
            else:
                # Strict mode - empty not allowed
                logging.error(f" CRITICAL: Record 1/{total_records}: Field '{field_name}' is EMPTY (not allowed)")
                logging.error(f" Location validation FAILED - Expected: '{reference_location}'")
                logging.error(f" STOPPING EXECUTION - Fix the data source configuration")
                return {
                    'error': True,
                    'client_found': None,
                    'valid': False,
                    'message': f"Field '{field_name}' is EMPTY (not allowed in strict mode)"
                }
        
        elif first_value != reference_location:
            invalid_count += 1
            client_found = first_value
            logging.error(f" Record 1/{total_records}: Field '{field_name}' = '{first_value}' (expected: '{reference_location}')")
            
            # Try second record if available
            if len(records) > 1:
                second_record = records[1]
                second_value = second_record.get(field_name, '').strip()
                
                if not second_value and allow_empty:
                    empty_count += 1
                    logging.warning(f"  Record 2/{total_records}: Field '{field_name}' is EMPTY (allowed by exception)")
                    logging.warning(f"  First record invalid, second empty, but continuing...")
                elif second_value != reference_location:
                    invalid_count += 1
                    if second_value:
                        client_found = second_value
                    logging.error(f" Record 2/{total_records}: Field '{field_name}' = '{second_value}' (expected: '{reference_location}')")
                    
                    if not allow_empty:
                        logging.error(f" CRITICAL: Location validation FAILED on both attempts")
                        logging.error(f" STOPPING EXECUTION - Wrong data source or configuration")
                        return {
                            'error': True,
                            'client_found': client_found,
                            'valid': False,
                            'message': f"Field '{field_name}' = '{client_found}' does not match expected '{reference_location}'"
                        }
                    else:
                        logging.warning(f"  Validation failed on both attempts, but continuing due to exception mode...")
                else:
                    valid_count += 1
                    client_found = second_value
                    logging.info(f" Record 2/{total_records}: Field '{field_name}' = '{second_value}' (VALID)")
            else:
                # Only one record and it's invalid
                if not allow_empty:
                    logging.error(f" CRITICAL: Only one record available and location is invalid")
                    logging.error(f" STOPPING EXECUTION")
                    return {
                        'error': True,
                        'client_found': client_found,
                        'valid': False,
                        'message': f"Only one record and field '{field_name}' = '{client_found}' does not match '{reference_location}'"
                    }
                else:
                    logging.warning(f"  Only one record and it's invalid, but continuing due to exception mode...")
        else:
            valid_count += 1
            client_found = first_value
            logging.info(f" Record 1/{total_records}: Field '{field_name}' = '{first_value}' (VALID)")
        
        # Summary
        logging.info(f" Location validation summary:")
        logging.info(f"   - Valid: {valid_count}")
        logging.info(f"   - Invalid: {invalid_count}")
        logging.info(f"   - Empty: {empty_count}")
        logging.info(f"   - Status: {' PASSED' if valid_count > 0 or allow_empty else ' FAILED'}")
        
        # Return result
        return {
            'error': False,
            'client_found': client_found,
            'valid': valid_count > 0 or allow_empty,
            'message': 'Validation passed' if valid_count > 0 else 'Validation passed with warnings'
        }