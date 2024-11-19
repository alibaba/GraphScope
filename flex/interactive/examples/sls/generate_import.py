import os
import sys


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python generate_import.py <input_dir>")
        sys.exit(1)

    input_dir = sys.argv[1]
    # find all files named like *__contains__*.csv
    # For each generate the output string and print to console.
    # for example, for the file acs@acs.alb.servergroup__contains__acs@acs.alb.listener.csv
    # generate the following string:
    #  - type_triplet:  
    #       edge: contains  
    #       source_vertex: acs@acs.alb.servergroup
    #       destination_vertex: acs.alb.listener
    #    inputs:  
    #       - acs@acs.alb.servergroup__contains__acs@acs.alb.listener.csv
    #    source_vertex_mappings:  
    #      - column:  
    #        index: 0  
    #    destination_vertex_mappings:  
    #      - column:  
    #        index: 1  
    edge_type = "__related_to__"
    
    files= [f for f in os.listdir(input_dir) if edge_type in f]
    for file in files:
        # remove .csv at tail
        source_vertex, destination_vertex = file.split(edge_type)
        destination_vertex=destination_vertex.replace(".csv", "")
        # print following string to console
        #  - destination_vertex: <>
        #    relation: MANY_TO_MANY
        #    source_vertex: <>
        print(f" - destination_vertex: {destination_vertex}")
        print(f"   relation: MANY_TO_MANY")
        print(f"   source_vertex: {source_vertex}")
        
        
        # edge_type_name = file.split("__")[1]
        # 
        # print(f" - type_triplet:  ")
        # print(f"      edge: {edge_type_name}")
        # print(f"      source_vertex: {source_vertex}")
        # print(f"      destination_vertex: {destination_vertex}")
        # print(f"   inputs:  ")
        # print(f"      - {file}")
        # print(f"   source_vertex_mappings:  ")
        # print(f"     - column:  ")
        # print(f"       index: 0  ")
        # print(f"   destination_vertex_mappings:  ")
        # print(f"     - column:  ")
        # print(f"       index: 1  ")
    
    