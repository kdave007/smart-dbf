import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from src.tables_schemas.simple import Simple

# Update these paths


dll_path = r"C:\Users\campo\Documents\projects\smart-dbf\Advantage.Data.Provider.dll"
dbf_path = "C:/Users/campo/Documents/dbf_encriptados/pospcp"  # Replace with actual path
password = "X3WGTXG5QJZ6K9ZC4VO2"      # Replace with actual password

simple = Simple(dbf_path, password, dll_path=dll_path)

# Filter by NO_REFEREN = '287732'
results = simple.get_table_data("VENTA", value_filters={"NO_REFEREN": "287732"})

print(f"Found {len(results)} records")
for record in results:
    print(record)
