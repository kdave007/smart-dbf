from src.tables_schemas.simple import Simple

def main():
    # Example usage of read_table method
    
    # Initialize Simple with your DBF file path and password
    data_source = "C:/Users/campo/Documents/dbf_encriptados/pospcp"  # Replace with actual path
    encryption_password = "X3WGTXG5QJZ6K9ZC4VO2"      # Replace with actual password
    
    simple = Simple(data_source, encryption_password)
    
    # # Example 1: Read all records from a table
    # print("=== Reading all records ===")
    # try:
    #     records = simple.read_dbf_table("VENTAS.DBF")  # Replace with actual table name
    #     print(f"Found {len(records)} records")
    #     if records:
    #         print("First record:", records[0])
    # except Exception as e:
    #     print(f"Error reading table: {e}")
    
    # Example 2: Read with limit
    print("\n=== Reading with limit (5 records) ===")
    try:
        records = simple.read_dbf_table("your_table_name", limit=5)
        print(f"Found {len(records)} records")
        for i, record in enumerate(records):
            print(f"Record {i+1}: {record}")
    except Exception as e:
        print(f"Error reading table: {e}")
    
    # Example 3: Read with filters
    print("\n=== Reading with filters ===")
    try:
        # Example filter: field equals value
        filters = [
            {"field": "some_field", "operator": "=", "value": "some_value"}
        ]
        records = simple.read_dbf_table("your_table_name", filters=filters)
        print(f"Found {len(records)} filtered records")
    except Exception as e:
        print(f"Error reading with filters: {e}")
    
    # Example 4: Read with date range filter
    print("\n=== Reading with date range filter ===")
    try:
        # Example date range filter
        filters = [
            {"field": "date_field", "operator": "range", "from_value": "2023-01-01", "to_value": "2023-12-31"}
        ]
        records = simple.read_dbf_table("your_table_name", filters=filters)
        print(f"Found {len(records)} records in date range")
    except Exception as e:
        print(f"Error reading with date range: {e}")

if __name__ == "__main__":
    main()
