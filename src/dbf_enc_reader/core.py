import clr
import json
import logging
from typing import List, Dict, Any, Optional
from pathlib import Path

from .connection import DBFConnection
from .converters import DataConverter
from src.utils.hash_manager import HashManager
from src.controllers.identifier_resolver import IdentifierResolver 

class DBFReader:
    def __init__(self, data_source: str, encryption_password: str = None, encrypted: bool = True):
        """
        Initialize DBF reader with connection parameters.
        
        Args:
            data_source: Path to the DBF file
            encryption_password: Password for encrypted DBF (optional if not encrypted)
            encrypted: Whether the DBF files are encrypted
        """
        # Log the data source path being used
        logging.info(f"Initializing DBFReader with data source: {data_source}")
        self.connection = DBFConnection(data_source, encryption_password, encrypted)
        self.converter = DataConverter()
        self.hash_manager = HashManager()
        self.resolver = IdentifierResolver()
        self._date_field_cache = {}  # Cache for date_field lookups

    def read_table(self, table_name: str, limit: Optional[int] = None, filters: Optional[List[Dict[str, Any]]] = None, include_recno: bool = True, select_fields: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """Read records from a table with optional filters.
        
        Args:
            table_name: Name of the table to read
            limit: Optional limit on number of records to read
            filters: Optional list of filter conditions
            include_recno: When True, include physical record number as metadata using RECNO()
            select_fields: Optional list of field names to include. If None, all fields are returned.
            
        Returns:
            List of records as dictionaries. When include_recno=True, each record will contain a
            "__meta" key with {"recno": <int>}.
        """

        results = []
        with self.connection as conn:
            from System.Data import CommandType
            
            # Always use TableDirect for compatibility with encrypted/local tables
            cmd = conn.conn.CreateCommand()
            cmd.CommandType = CommandType.TableDirect
            cmd.CommandText = table_name
            cmd.AdsOptimizedFilters = True  # Enable AOF for better performance

            # Get Advantage Extended Reader
            reader = cmd.ExecuteExtendedReader()

            # Apply filters if any via AOF
            if filters:
                filter_conditions = []
                use_or = len(filters) > 1 and all(f['field'] == filters[0]['field'] for f in filters)
                
                for f in filters:
                    # print(f' filter ////// {f}')
                    if f['operator'] == 'range':
                        filter_conditions.append(
                            f"{f['field']} >= '{f['from_value']}' AND {f['field']} <= '{f['to_value']}'"
                        )
                    else:
                        filter_conditions.append(
                            f"{f['field']} {f['operator']} '{f.get('value', '')}'"
                        )

                # print(f'HERE ------ {filter_conditions}')        
                
                if filter_conditions:
                    join_op = " OR " if use_or else " AND "
                    filter_expr = join_op.join(filter_conditions)
                    # print(f"\nApplying AOF filter: {filter_expr}")
                    
                    try:
                        reader.Filter = filter_expr
                    except Exception as e:
                        print(f"\nFilter error: {str(e)}")
                        print(f"Filter expression: {filter_expr}")
                        raise

            # Pre-compute field indices if select_fields is specified
            field_indices = {}
            if select_fields:
                for field in select_fields:
                    try:
                        field_indices[field] = reader.GetOrdinal(field)
                    except Exception:
                        # Field doesn't exist in table, skip it
                        pass

            # Process results and attach physical identifier metadata when requested
            count = 0
            while reader.Read():
                if limit and count >= limit:
                    break

                record = {}
                if select_fields and field_indices:
                    # Only read specified fields using pre-computed indices
                    for field_name, index in field_indices.items():
                        try:
                            value = reader.GetValue(index)
                            if isinstance(value, str):
                                value = value.strip()
                            record[field_name] = self.converter.convert_value(value)
                        except Exception:
                            # Skip if field can't be read
                            pass
                else:
                    # Read all fields (current behavior)
                    for i in range(reader.FieldCount):
                        field_name = reader.GetName(i)
                        value = reader.GetValue(i)
                        record[field_name] = self.converter.convert_value(value)

                if include_recno:
                    # Try to obtain Advantage extended reader physical identifiers
                    recno_val = None
                    rowid_hex = None
                    try:
                        # Some providers expose RecordNumber on the extended reader
                        if hasattr(reader, 'RecordNumber'):
                            recno_val = int(reader.RecordNumber)
                    except Exception:
                        recno_val = None
                    try:
                        # Bookmark is a stable binary identifier; attach as hex if available
                        if hasattr(reader, 'Bookmark'):
                            bm = reader.Bookmark
                            if bm is not None:
                                rowid_hex = bm.ToString() if hasattr(bm, 'ToString') else (bm.hex() if hasattr(bm, 'hex') else str(bm))
                    except Exception:
                        rowid_hex = None

                    meta = {}

                    # Generate hash of record (excluding recno)
                    record_hash = self.hash_manager.hash_record(record)

                    #calculate id by strategy
                    strategy = self.resolver.resolve_identifier_strategy(table_name)
                    field_name = strategy.get_sql_field_id_name()
                    # print(f' STRATEGY NAME FOR TABLE {table_name} :: {strategy.get_strategy_name()}')

                    if recno_val is not None:
                        #we need to check if it uses recno as id, cause its not already set in the record yet, so cant be calculated, we get the recno and add it directly
                        if strategy.get_strategy_name() == 'physical_position':
                            meta[field_name] = recno_val
                        else:
                            meta[field_name] = strategy.calculate_identifier(record)
                            meta['recno'] = recno_val
                    
                    if rowid_hex is not None:
                        meta['rowid'] = rowid_hex

                    meta['hash_comparador'] = record_hash

                    # Add ref_date if date_field is configured for this table
                    date_field = self._get_date_field(table_name)
                    if date_field and date_field in record:
                        meta['ref_date'] = record[date_field]

                    if meta:
                        record['__meta'] = meta

                results.append(record)
                count += 1
            return results
    
    def _get_date_field(self, table_name: str) -> Optional[str]:
        """
        Get the date_field from rules.json for a given table.
        Uses caching to avoid repeated file reads.
        
        Args:
            table_name: Name of the table
            
        Returns:
            The date field name or None if not configured
        """
        # Check cache first
        if table_name in self._date_field_cache:
            return self._date_field_cache[table_name]
        
        # Load from rules.json
        try:
            rules_path = Path(__file__).parent.parent / "utils" / "rules.json"
            with open(rules_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                table_config = data.get(table_name, {})
                date_field = table_config.get('date_field', None)
                
                # Cache the result (even if None)
                self._date_field_cache[table_name] = date_field
                return date_field
        except Exception as e:
            logging.warning(f"[DBFReader] Could not load date_field for {table_name}: {e}")
            self._date_field_cache[table_name] = None
            return None

    def to_json(self, table_name: str, limit: Optional[int] = None, filters: Optional[List[Dict[str, Any]]] = None) -> str:
        """
        Convert table records to JSON string.
        
        Args:
            table_name: Name of the table to convert
            limit: Optional limit on number of records to convert
            filters: Optional list of filter conditions
            
        Returns:
            JSON string representation of the records
        """
        records = self.read_table(table_name, limit, filters)
        #print(self.get_table_info(table_name))

        # print(f' records  {records}')
        
        return json.dumps(records, indent=4, ensure_ascii=False)

    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """
        Get information about table structure.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Dictionary containing table metadata
        """
        with self.connection as conn:
            reader = conn.get_reader(table_name)
            return {
                'field_count': reader.FieldCount,
                'columns': [reader.GetName(i) for i in range(reader.FieldCount)]
            }
