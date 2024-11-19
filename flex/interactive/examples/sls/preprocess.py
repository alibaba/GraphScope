#/bin/python3

import sys
import os
import csv


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python3 preprocess.py <input_file> <output_directory>")
        sys.exit(1)

    input_file = sys.argv[1]
    output_dir = sys.argv[2]

    # Read the input csv file, and split it into multiple files: vertex files and edge files.
    # The header is like "__src_domain__","__src_entity_type__","__src_entity_id__","__relation_type__","__dest_domain__","__dest_entity_type__","__dest_entity_id__"
    # Each line is a edge relation, we need to use a dictionary to store the vertex information.
    # The vertex information is like "__domain__","__entity_type__","__entity_id__"
    # The vertices should be clustered by __domain__@__entity_type__

    # The output files should be named as:
    # __domain__@__entity_type__.csv
    # The edges should be stored in the edge file.
    # The edges should be clustered by __src_domain__@__src_entity_type__ and __dest_domain__@__dest_entity_type__ and __relation_type__
    
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # if the output dir is not empty, exit
    if len(os.listdir(output_dir)) > 0:
        print("Output directory is not empty")
        sys.exit(1)
        
    vertex_dict = {}
    edge_dict = {}
    with open(input_file, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            src_domain = row["__src_domain__"]
            src_entity_type = row["__src_entity_type__"]
            src_entity_id = row["__src_entity_id__"]
            relation_type = row["__relation_type__"]
            dest_domain = row["__dest_domain__"]
            dest_entity_type = row["__dest_entity_type__"]
            dest_entity_id = row["__dest_entity_id__"]

            src_key = src_domain + "_" + src_entity_type
            dest_key = dest_domain + "_" + dest_entity_type
            # replace . with _
            src_key = src_key.replace(".", "_")
            dest_key = dest_key.replace(".", "_")

            if src_key not in vertex_dict:
                vertex_dict[src_key] = set()
            if dest_key not in vertex_dict:
                vertex_dict[dest_key] = set()

            vertex_dict[src_key].add((src_entity_id))
            vertex_dict[dest_key].add((dest_entity_id))

            edge_key = src_key + "__" + relation_type + "__" + dest_key
            edge_key = edge_key.replace(".", "_")
            if edge_key not in edge_dict:
                edge_dict[edge_key] = set()
            edge_dict[edge_key].add((src_entity_id, dest_entity_id))
    
    total_vertex = 0   
    total_edges = 0   
    for key in vertex_dict:
        # Write the vertex file
        vertex_file = os.path.join(output_dir, key + ".csv")
        with open(vertex_file, "w") as f:
            f.write("__entity_id__\n")
            for entity_id in vertex_dict[key]:
                f.write(entity_id + "\n")
        print("Write vertex file: " + vertex_file + ", number of vertices: " + str(len(vertex_dict[key])))
        total_vertex += len(vertex_dict[key])
        
    for key in edge_dict:
        # replaces contains with contain in key
        output_key = key.replace("contains", "contain")
        # Write the edge file
        edge_file = os.path.join(output_dir, output_key + ".csv")
        with open(edge_file, "w") as f:
            f.write("__src_entity_id__,__dest_entity_id__\n")
            for src_entity_id, dest_entity_id in edge_dict[key]:
                f.write(src_entity_id + "," + dest_entity_id + "\n")
        print("Write edge file: " + edge_file + ", number of edges: " + str(len(edge_dict[key])))
        total_edges += len(edge_dict[key])
        
    print("Total number of vertices: " + str(total_vertex))
    print("Total number of edges: " + str(total_edges))
        
    

       
    

    