#!/bin/python3

import os
import sys

if __name__ == "__main__":
    # Expect a arg of file path
    if len(sys.argv) != 4:
        print("Usage: python3 preprocess.py <file> <vertex_file> <edge_file>")
        sys.exit(1)
    # Get the file path
    file_path = sys.argv[1]
    vertex_file_path = sys.argv[2]
    edge_file_path = sys.argv[3]
    vertices = set()
    edges = []
    # open the file and iterate over the lines
    with open(file_path, "r") as file:
        for line in file:
            # if line starts with #, skip it
            if line.startswith("#"):
                continue
            # split the line by space
            parts = line.split()
            # if contains two parts, it is a edge
            if len(parts) == 2:
                int_parts = [int(part) for part in parts]
                # add the vertices to the set
                vertices.add(int_parts[0])
                vertices.add(int_parts[1])
                edges.append(parts)
    # write vertices to vertices.csv, and edges to edges.csv
    # sort vertices
    vertices = sorted(vertices)
    with open(vertex_file_path, "w") as file:
        for vertex in vertices:
            file.write(str(vertex) + "\n")
    with open(edge_file_path, "w") as file:
        for edge in edges:
            file.write(edge[0] + "," + edge[1] + "\n")
                
            
        