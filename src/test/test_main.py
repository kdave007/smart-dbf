import sys
import os
from pathlib import Path

# Add the project root directory to the Python path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

from src.tables_schemas.simple import Simple


def test_with_custom_args(data_source, password, date_from=None, date_to=None, table_name="VENTA.DBF", dll_path=None):
    """Test with custom arguments provided by user"""
    
    try:
        print(f"Testing with custom args:")
        print(f"- Data source: {data_source}")
        print(f"- Table: {table_name}")
        if date_from and date_to:
            print(f"- Date range: {date_from} to {date_to}")
        
        # Initialize controller with exe-compatible paths
        import sys
        
        # Get base path for exe or development
        if getattr(sys, 'frozen', False):
            # Running as exe
            base_path = Path(sys.executable).parent
        else:
            # Running in development
            base_path = project_root
        
        rules_path = base_path / "src" / "utils" / "rules.json"
        mappings_path = base_path / "src" / "utils" / "mappings.json"
        
        controller = Simple(
            data_source=data_source,
            encryption_password=password,
            mapping_file_path=str(mappings_path),
            dll_path=dll_path,
            filters_file_path=str(rules_path),
            encrypted=True
        )
        
        # Show table info
        print(f"\n=== Table Info for {table_name} ===")
        info = controller.get_table_info(table_name)
        print(f"Total fields: {info.get('field_count', 'N/A')}")
        print(f"Mapped fields: {info.get('total_mapped_fields', 'N/A')}")
        if 'mapped_fields' in info:
            print("Available fields:", ", ".join(info['mapped_fields']))
        
        # Test without date filter first
        print(f"\n=== Sample records from {table_name} (no filter) ===")
        
        sample_data = controller.get_table_data(table_name=table_name, limit=2)
        for i, record in enumerate(sample_data, 1):
            print(f"Record {i}: {record}")
        
        # Test with date filter if provided
        if date_from and date_to:
            print(f"\n=== Records from {table_name} with date filter ({date_from} to {date_to}) ===")
            filtered_data = controller.get_table_data(
                table_name=table_name,
                date_range={"from": date_from, "to": date_to},
                limit=None
            )
            print(f"Found {len(filtered_data)} records")
            for i, record in enumerate(filtered_data, 1):
                print(f"Record {i}: {record.get('NO_REFEREN')} - index = {record.get('__meta')}")
        else:
            print(f"\nNo date filter applied.")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    print("=== Simple DBF Controller Test ===")
    enc_data_src = r"C:\Users\campo\Documents\dbf_encriptados\pospcp"
    data_src = r"C:\Users\campo\Documents\projects\data_sucursales\arauc"
  
    test_with_custom_args(
        data_source=enc_data_src,
        password="X3WGTXG5QJZ6K9ZC4VO2",  # ← Replace with your real password
        date_from="2025-01-01",  # Changed to match your actual data date
        date_to="2025-04-30",
        table_name="VENTA",
        dll_path=r"C:\Users\campo\Documents\projects\smart-dbf\Advantage.Data.Provider.dll",
    )
