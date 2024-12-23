#!/bin/python3
import sys
import csv


if __name__ == "__main__":
    # expect two args, the actual file and expected file
    
    if len(sys.argv) != 3:
        print("Usage: sort_and_compare.py <actual> <expected>")
        sys.exit(1)
    
    actual_file = sys.argv[1]
    expected_file = sys.argv[2]
    # Each csv files contains two columns, the first column is the cluster id, and second column is the vertex id.
    # Read the actual file and expected file into two lists
    actual = {}
    actual_cluster_map = {}
    expected = {}
    expected_cluster_map = {}
    with open(actual_file, 'r') as f:
        reader = csv.reader(f,delimiter=' ')
        for row in reader:
            vertex_id = int(row[0])
            cluster_id = int(row[1])
            actual[vertex_id] = cluster_id
            if actual_cluster_map.get(cluster_id) is None:
                actual_cluster_map[cluster_id] = 0
            actual_cluster_map[cluster_id] += 1
    print("Finished reading actual file, vertices count: ", len(actual), " num clusters ", len(actual_cluster_map), " min cluster size, max cluster size: ", min(actual_cluster_map.values()), max(actual_cluster_map.values()))

    with open(expected_file, 'r') as f:
        reader = csv.reader(f, delimiter='\t')
        for row in reader:
            vertex_id = int(row[0])
            cluster_id = int(row[1])
            expected[vertex_id] = cluster_id
            if expected_cluster_map.get(cluster_id) is None:
                expected_cluster_map[cluster_id] = 0
            expected_cluster_map[cluster_id] += 1

        # cur_cluster = 0
        # for line in f:
        #     # each line is like Category:Buprestoidea; 301 302 303 304 305 306 30
        #     # got all numbers after; and put them into the same cluster
        #     vertex_ids = line.split(";")[1].split()
        #     for vertex_id in vertex_ids:
        #         expected[int(vertex_id)] = cur_cluster
        #     expected_cluster_map[cur_cluster] = len(vertex_ids)
        #     cur_cluster += 1
            
    print("Finished reading expected file, vertices count: ", len(expected), " num clusters", len(expected_cluster_map),  " min cluster size, max cluster size: ", min(expected_cluster_map.values()), max(expected_cluster_map.values()))
    actual_cluster_id_to_expected_cluster_id = {}
    cnt = 0
    for vertex_id, cluster_id in actual.items():
        if vertex_id in expected:
            if actual_cluster_id_to_expected_cluster_id.get(cluster_id) is None:
                actual_cluster_id_to_expected_cluster_id[cluster_id] = expected[vertex_id]
            if actual_cluster_id_to_expected_cluster_id[cluster_id] != expected[vertex_id]:
                cnt += 1
                # print("Cluster id mismatch for vertex id: ", vertex_id, " actual cluster id: ", cluster_id, " expected cluster id: ", expected[vertex_id])
        else:
            print("Vertex id not found in expected file: ", vertex_id)
            
    # count the number of cluster with size >2
    expected_cluster_ids_appeared_in_actual = set()
    cnt_2 = 0
    for cluster_id, size in actual_cluster_map.items():
        if size >= 2:
            cnt_2 += 1
            expected_cluster_id =  actual_cluster_id_to_expected_cluster_id.get(cluster_id)
            expected_cluster_ids_appeared_in_actual.add(expected_cluster_id)
    print("Number expect cluster with size >= 2 appeared in actual: ", cnt_2, " out of ", len(expected_cluster_map))
            
    print("Total cluster id mismatch: ", cnt, " out of ", len(expected), "expected cluster number: ", len(actual_cluster_id_to_expected_cluster_id), " actual cluster number: ", len(expected_cluster_map))
    
         