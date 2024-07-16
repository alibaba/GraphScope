# A utility to help generate schema.cypher and dataloading.cypher as metadata required by kuzu, from the schema.yaml defined in GIE.

import yaml
import os
import sys
import argparse

def generate_meta(schema_file, csv_directory, case_option, suffix, edge_naming):
    with open(schema_file, 'r') as file:
        schema = yaml.safe_load(file)
    
    create_statements = []
    copy_statements = []

    def get_edge_name(type_name, source_vertex, destination_vertex):
        if edge_naming == "src_dst":
            return f"{source_vertex}_{type_name}_{destination_vertex}"
        else:
            return type_name

    def get_csv_path(type_name, source_vertex=None, destination_vertex=None, is_edge=False):
        if is_edge:
            named_type = get_edge_name(type_name, source_vertex, destination_vertex)
        else:
            named_type = type_name
        
        if case_option == "upper":
            csv_file_name = f"{named_type.upper()}{suffix}.csv"
        elif case_option == "lower":
            csv_file_name = f"{named_type.lower()}{suffix}.csv"
        elif case_option == "camel":
            csv_file_name = f"{named_type.capitalize()}{suffix}.csv"
        else:
            csv_file_name = f"{named_type}{suffix}.csv"
        return os.path.join(csv_directory, csv_file_name)
    
    # Generating meta for vertex types
    for vertex in schema['schema']['vertex_types']:
        type_name = vertex['type_name']
        properties = vertex['properties']
        primary_keys = vertex.get('primary_keys', [])
        
        columns = []
        for prop in properties:
            prop_name = prop['property_name']
            prop_type = prop['property_type']
            if 'primitive_type' in prop_type:
                if prop_type['primitive_type'] == 'DT_SIGNED_INT64':
                    columns.append(f"{prop_name} INT64")
                elif prop_type['primitive_type'] == 'DT_SIGNED_INT32':
                    columns.append(f"{prop_name} INT32")
                # Add more primitive types as needed
            elif 'string' in prop_type:
                columns.append(f"{prop_name} STRING")
        
        primary_keys_str = ', '.join(primary_keys)
        columns_str = ', '.join(columns)
        create_table_statement = f"CREATE NODE TABLE {type_name}({columns_str}, PRIMARY KEY ({primary_keys_str}))"
        create_statements.append(create_table_statement)

        copy_statement = f'COPY {type_name} FROM "{get_csv_path(type_name)}"'
        copy_statements.append(copy_statement)

    # Generating meta for edge types
    for edge in schema['schema']['edge_types']:
        type_name = edge['type_name']
        properties = edge.get('properties', [])
        
        for vertex_pair in edge['vertex_type_pair_relations']:
            source_vertex = vertex_pair['source_vertex']
            destination_vertex = vertex_pair['destination_vertex']
            edge_name = get_edge_name(type_name, source_vertex, destination_vertex)

            columns = []
            for prop in properties:
                prop_name = prop['property_name']
                prop_type = prop['property_type']
                if 'primitive_type' in prop_type:
                    if prop_type['primitive_type'] == 'DT_SIGNED_INT64':
                        columns.append(f"{prop_name} INT64")
                    elif prop_type['primitive_type'] == 'DT_SIGNED_INT32':
                        columns.append(f"{prop_name} INT32")
                    # Add more primitive types as needed
                elif 'string' in prop_type:
                    columns.append(f"{prop_name} STRING")
            
            columns_str = ', '.join(columns)
            if columns_str:
                create_rel_statement = f"CREATE REL TABLE {edge_name}(FROM {source_vertex} TO {destination_vertex}, {columns_str})"
            else:
                create_rel_statement = f"CREATE REL TABLE {edge_name}(FROM {source_vertex} TO {destination_vertex})"
            create_statements.append(create_rel_statement)

            copy_statement = f'COPY {edge_name} FROM "{get_csv_path(type_name, source_vertex, destination_vertex, is_edge=True)}"'
            copy_statements.append(copy_statement)
    
    return create_statements, copy_statements

def main():
    parser = argparse.ArgumentParser(description="Generate SQL statements from schema.yaml.")
    parser.add_argument("schema_file", help="Path to the schema.yaml file.")
    parser.add_argument("csv_directory", help="Directory containing the CSV files.")
    parser.add_argument("--case", choices=["upper", "lower", "camel"], default=None, 
                        help="Determines the case of the CSV file names. Can be 'upper', 'lower', or 'camel'. Defaults to type name case.")
    parser.add_argument("--suffix", default="", 
                        help="Optional suffix to add to the CSV file names.")
    parser.add_argument("--edge-naming", choices=["type", "src_dst"], default="type",
                        help="Determines the naming convention for edge CSV files. Can be 'type' or 'src_dst'. Defaults to 'type'.")
    
    args = parser.parse_args()

    create_statements, copy_statements = generate_meta(args.schema_file, args.csv_directory, args.case, args.suffix, args.edge_naming)
    
    with open("schema.cypher", "w") as schema_file:
        for statement in create_statements:
            schema_file.write(f"{statement};\n")
    
    with open("copy.cypher", "w") as copy_file:
        for statement in copy_statements:
            copy_file.write(f"{statement};\n")

if __name__ == "__main__":
    main()
