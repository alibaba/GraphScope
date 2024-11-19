#/bin/python3

import json
import sys
import os
import csv

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: python3 process_entity.py <input_file_num> <input_files...> <output_directory>")
        sys.exit(1)
        
    input_file_num = int(sys.argv[1])
    input_files = sys.argv[2:2+input_file_num]
    output_dir = sys.argv[2+input_file_num]
    
    # The input files are all json files, each file is a list of entities.
    # Each json object contains a __domain__ field, a __entity_type__ field, and a __entity_id__ field.
    # The objects should be clustered by __domain_____entity_type__.
    json_objects = {}
    obj_cnts = 0
    for input_file in input_files:
        with open(input_file, "r") as f:
            entities = json.load(f)
            for entity in entities:
                domain = entity["__domain__"]
                entity_type = entity["__entity_type__"]
                key = domain + "_" + entity_type
                # replace . with _
                key = key.replace(".", "_")
                if key not in json_objects:
                    json_objects[key] = []
                json_objects[key].append(entity)
                obj_cnts += 1
    print("Total objects: ", obj_cnts)
    # For each key, dump the objects into a csv file.
    # the result csv file is named into __domain_____entity_type__.csv. The fields are all the fields in the json object.
    if os.path.exists(output_dir):
        print("Output directory is not empty")
        sys.exit(1)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    for key, objs in json_objects.items():
        output_file = os.path.join(output_dir, key + ".csv")
        local_cnt = 0
        with open(output_file, "w") as f:
            filed_names = objs[0].keys()
            writer = csv.DictWriter(f, fieldnames=filed_names)
            writer.writeheader()
            for obj in objs:
                # if obj contains field that are not in fieldnames, ignore it
                obj = {k: v for k, v in obj.items() if k in filed_names}
                writer.writerow(obj)
                local_cnt += 1
        print("Write to ", output_file, ", total num rows: ", local_cnt)
    print("Done")
            
    