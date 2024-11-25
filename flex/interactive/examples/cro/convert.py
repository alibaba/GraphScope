#!/bin/python3

if __name__ == "__main__":
    # Expect two arguments: input and output file
    # The input file is a csv consists of three columns, src, dst, and weight
    # Extract the src and dst columns, dedup, and output to the output file
    import sys
    import pandas as pd
    
    if len(sys.argv) != 3:
        print("Usage: python convert.py input.csv output.csv")
        sys.exit(1)
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    
    df = pd.read_csv(input_file, header=None, names=['src', 'dst', 'weight'])
    # merge src and dst columns, and dedup
    df = pd.concat([df['src'], df['dst']]).drop_duplicates()
    df.to_csv(output_file, index=False, header=False)
    
    print(f"Converted {input_file} to {output_file}")
    
    
    