import sys
from pathlib import Path

# Add the project root directory to the Python path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

from src.tables_schemas.composed import Composed


def test_composed_get_table_data(
    data_source,
    password,
    table,
    dll_path=None,
    encrypted=True,
    date_from=None,  
    date_to=None
):
    """Simple test for Composed.get_table_data

    Args:
        data_source: Path to the DBF folder
        password: Encryption password
        table_name: The main target table name
        related_table: The related/reference table name
        matching_field: The field name to match between tables
        dll_path: Optional path to Advantage.Data.Provider.dll
        encrypted: Whether the DBF files are encrypted
    """
    try:
        # Determine base path for rules/mappings depending on exe vs dev
        if getattr(sys, 'frozen', False):
            base_path = Path(sys.executable).parent
        else:
            base_path = project_root

        rules_path = base_path / "src" / "utils" / "rules.json"
        mappings_path = base_path / "src" / "utils" / "mappings.json"

        controller = Composed(
            data_source=data_source,
            encryption_password=password,
            mapping_file_path=str(mappings_path),
            dll_path=dll_path,
            filters_file_path=str(rules_path),
            encrypted=encrypted,
        )

        print("\n=== Composed.get_table_data call ===")
        print(f"Main table: {table}")


        data = controller.get_table_data(
            table = table,
            date_range={"from": date_from, "to": date_to},
            limit=None
        )

        if isinstance(data, list):
            print(f"Returned {len(data)} reference records")
            for i, row in enumerate(data[:], 1):
                print(f"Ref {row.get('NO_REFEREN')}: {row.get('__meta')}")
        else:
            print("Returned:", data)

    except Exception as e:
        print(f"❌ Error in test_composed_get_table_data: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    print("=== Composed Controller Simple Test ===")
    # Adjust these defaults to your environment as needed
    enc_data_src = r"C:\\Users\\campo\\Documents\\dbf_encriptados\\pospcp"
    data_src = r"C:\\Users\\campo\\Documents\\projects\\data_sucursales\\arauc"

    test_composed_get_table_data(
        data_source=data_src,
        password="X3WGTXG5QJZ6K9ZC4VO2",  # ← Replace with your real password
        table="PARTVTA",               # main table you want to explore
        dll_path=r"C:\Users\campo\Documents\projects\smart-dbf\Advantage.Data.Provider.dll",
        encrypted=False,
        date_from="2025-01-01",  # Changed to match your actual data date
        date_to="2025-09-25"
    )
