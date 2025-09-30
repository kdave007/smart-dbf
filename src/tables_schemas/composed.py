from numbers import Number
from ..dbf_enc_reader.mapping_manager import MappingManager
from pathlib import Path
import hashlib
import json
from ..tables_schemas.simple import Simple
import sys
import time

class Composed:
    def __init__(self, data_source: str, encryption_password: str, mapping_file_path: str = None, dll_path: str = None, filters_file_path: str = None, encrypted: bool = False):
        self.table= None
        self.matching_field = None
        self.simple_controller = Simple(data_source, encryption_password, mapping_file_path, dll_path, filters_file_path, encrypted)


    def _get_composed_parent_fields(self, table_name: str):
        """Get composed_parent profile fields from field_map.json"""
        try:
            field_map_path = Path(__file__).parent.parent / "utils" / "rules.json"
            with open(field_map_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                table_key = table_name
                table_config = data.get(table_key, {})
                composed_parent = table_config.get('composed_parent', {})

                print(f"[Composed] Found composed_parent for {table_name}: {composed_parent}")
                return composed_parent
        except Exception as e:
            print(f"[Composed] Error loading field_map.json: {e}")
            return []
    
    def _get_references(self, related_params, date_range, limit):

        target_table = related_params.get("related_table",None)
        fields_to_select = related_params.get("fields", [])
    
        # Pass select_fields to only read specific columns
        references = self.simple_controller.get_table_data(
            target_table, 
            limit, 
            date_range, 
            select_fields=fields_to_select if fields_to_select else None
        )
        print(f' total references {len(references)}')

       
        
        # Sort references by the first field in fields_to_select (usually the key field)
        if references and fields_to_select:
            sort_field = fields_to_select[0]  # Use first field for sorting
            try:
                references.sort(key=lambda x: x.get(sort_field, ''))
                print(f"[Composed] References sorted by {sort_field}")
            except Exception as e:
                print(f"[Composed] Could not sort references by {sort_field}: {e}")
        
        # print(f' REF {references[0]}')
        return references

    def get_table_data(self, table, date_range, limit: int = 300):
        """
            TODO here i need to fetch data only by the reference matching field and then search in the actual target table 
        """
        print(f" GET TABLE DATA FOR {table}")
        related_params = self._get_composed_parent_fields(table)
        references = self._get_references( related_params,date_range, limit)
        matching_field = related_params.get('matching_field')
        # Handle matching_field as list or string
        if isinstance(matching_field, list) and matching_field:
            matching_field = matching_field[0]  # Take first element if it's a list

        # Use chunked method for better performance with large datasets
        results = self.get_by_references_chunked(references, matching_field, table, chunk_size=2000)
        self._debug_match_summary(references, results, matching_field) 
        # sys.exit()   
            
        return results


    def get_by_references_loop(self, references, matching_field, target_table):
        """
        Fetch records using individual queries for each reference value.
        Less efficient but more reliable for very large datasets (50k+ records).
        """
        if not references:
            return []
        
        # Extract unique reference values
        ref_values = set()
        for ref in references:
            if matching_field in ref and ref[matching_field] is not None:
                ref_values.add(str(ref[matching_field]))
        
        if not ref_values:
            print(f"[Composed] No valid reference values found for field '{matching_field}'")
            return []
        
        print(f"[Composed] Processing {len(ref_values)} references individually")
        
        all_results = []
        processed = 0
        
        for value in ref_values:
            # Single value filter for each reference
            value_filters = {matching_field: value}
            
            try:
                results = self.simple_controller.get_table_data(
                    target_table, 
                    value_filters=value_filters
                )
                all_results.extend(results)
                processed += 1
                
                # Progress indicator for large datasets
                if processed % 1000 == 0:
                    print(f"[Composed] Processed {processed}/{len(ref_values)} references, found {len(all_results)} total records")
                    
            except Exception as e:
                print(f"[Composed] Error processing reference {value}: {e}")
                continue
        
        print(f"[Composed] Completed processing {processed} references, found {len(all_results)} total records")
        
        # Sort by matching_field (folio)
        if all_results and matching_field:
            try:
                all_results.sort(key=lambda x: x.get(matching_field, ''))
                print(f"[Composed] Results sorted by {matching_field}")
            except Exception as e:
                print(f"[Composed] Could not sort by {matching_field}: {e}")
        
        return all_results

    def get_by_references_chunked(self, references, matching_field, target_table, chunk_size=500):
        """
        Fetch records using chunked OR filters to balance performance and reliability.
        Processes references in chunks to avoid filter length limits while being faster than individual queries.
        """
        if not references:
            return []
        
        # Extract unique reference values

        

        ref_values = list(set(str(ref[matching_field]) for ref in references 
                             if matching_field in ref and ref[matching_field] is not None))
        
        if not ref_values:
            print(f"[Composed] No valid reference values found for field '{matching_field}'")
            return []
        
        print(f"[Composed] Processing {len(ref_values)} references in chunks of {chunk_size}")
        
        # Start timer
        start_time = time.time()
        
        all_results = []
        total_chunks = (len(ref_values) + chunk_size - 1) // chunk_size  # Ceiling division
        
        for i in range(0, len(ref_values), chunk_size):
            chunk = ref_values[i:i + chunk_size]
            chunk_num = i // chunk_size + 1
            
            # Build OR filter for this chunk
            filters = []
            for value in chunk:
                filters.append({
                    'field': f"ALLTRIM({matching_field})",
                    'operator': '=',
                    'value': value
                })
            
            try:
                # Single call with OR filter for this chunk
                results = self.simple_controller.read_dbf_table(target_table, filters=filters)
                all_results.extend(results)
                print(f"[Composed] Chunk {chunk_num}/{total_chunks}: {len(chunk)} refs -> {len(results)} records")
                
            except Exception as e:
                print(f"[Composed] Error processing chunk {chunk_num}: {e}")
                # Fallback to individual queries for this chunk
                print(f"[Composed] Falling back to individual queries for chunk {chunk_num}")
                for value in chunk:
                    try:
                        value_filters = {matching_field: value}
                        results = self.simple_controller.get_table_data(target_table, value_filters=value_filters)
                        all_results.extend(results)
                    except Exception as e2:
                        print(f"[Composed] Error processing individual reference {value}: {e2}")
                        continue
        
        # End timer and calculate duration
        end_time = time.time()
        duration = end_time - start_time
        
        print(f"[Composed] Completed chunked processing in {duration:.2f} seconds, found {len(all_results)} total records")
        print(f"[Composed] Average time per chunk: {duration/total_chunks:.2f} seconds")
        
        # Sort by matching_field (folio)
        if all_results and matching_field:
            try:
                all_results.sort(key=lambda x: x.get(matching_field, ''))
                print(f"[Composed] Results sorted by {matching_field}")
            except Exception as e:
                print(f"[Composed] Could not sort by {matching_field}: {e}")
        
        return all_results

    def _debug_match_summary(self, references, results, matching_field):
        """
        Debug method to show how many records were found for each reference value.
        
        Args:
            references: List of reference records (parent table)
            results: List of result records (child table) 
            matching_field: Field name to match on
            
        Returns:
            Dict with reference values as keys and count of matches as values
        """
        if not references or not results or not matching_field:
            print("[Debug] Missing data for match summary")
            return {}
        
        # Extract reference values
        ref_values = set()
        for ref in references:
            if matching_field in ref and ref[matching_field] is not None:
                ref_values.add(str(ref[matching_field]))
        
        # Count matches for each reference
        match_counts = {}
        for ref_value in ref_values:
            count = sum(1 for result in results 
                       if str(result.get(matching_field, '')) == ref_value)
            match_counts[ref_value] = count
        
        # Print summary
        print(f"\n[Debug] Match Summary for field '{matching_field}':")
        print(f"[Debug] Total references: {len(ref_values)}")
        print(f"[Debug] Total results: {len(results)}")
        print("[Debug] Matches per reference:")
        
        # for ref_value in sorted(match_counts.keys()):
        #     count = match_counts[ref_value]
        #     print(f"[Debug] ref {ref_value}: {count}")
        
        # Summary stats
        total_matches = sum(match_counts.values())
        refs_with_matches = sum(1 for count in match_counts.values() if count > 0)
        refs_without_matches = len(ref_values) - refs_with_matches
        
        print(f"[Debug] Summary: {refs_with_matches} refs with matches, {refs_without_matches} refs without matches")
        print(f"[Debug] Total matched records: {total_matches}")
        
        return match_counts

    def trim_references(self, references, fields):
        if not references or not fields:
            return references
            
        # Extract trim configuration from fields
        trim_config = {}
        for field_config in fields:
            if isinstance(field_config, dict):
                field_name = field_config.get('field_name')
                trim_string = field_config.get('trim_string', False)
                if field_name and trim_string:
                    trim_config[field_name] = True
        
        # Apply trimming if any fields are configured for it
        if trim_config:
            for ref in references:
                for field_name, should_trim in trim_config.items():
                    if should_trim and field_name in ref and isinstance(ref[field_name], str):
                        ref[field_name] = ref[field_name].strip()
        
        return references

    def _get_filter_config(self, table_name: str, field_name: str):
        """Get filter configuration for a specific field from rules.json"""
        try:
            field_map_path = Path(__file__).parent.parent / "utils" / "rules.json"
            with open(field_map_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                table_config = data.get(table_name, {})
                filters = table_config.get('filters', {})
                value_filter = filters.get('value', {})
                
                if value_filter.get('field') == field_name:
                    condition = value_filter.get('condition', 'equal')
                    
                    # Map condition to operator - Advantage doesn't support LIKE in filters
                    if condition == "equal":
                        return {"operator": "="}
                    elif condition == "contains":
                        return {"operator": "=", "contains_mode": True}  # Use = but filter in memory
                    elif condition == "starts_with":
                        return {"operator": "=", "starts_mode": True}
                    elif condition == "ends_with":
                        return {"operator": "=", "ends_mode": True}
                
                return {"operator": "="}  # Default to equal
        except Exception as e:
            print(f"[Composed] Error loading filter config: {e}")
            return {"operator": "="}

        
        
        

