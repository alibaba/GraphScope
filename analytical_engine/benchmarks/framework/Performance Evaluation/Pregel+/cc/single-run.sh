make clean
make

# hdfs dfs -rm /pregel+_data/toy_graph.txt
# hdfs dfs -put toy_graph.txt /pregel+_data/

# hdfs dfs -cat /pregel+_data/toy_graph.txt
# mpiexec -n 2 ./run /pregel+_data/toy_graph.txt
# hdfs dfs -ls /pregel+_data/toy_output/
# hdfs dfs -cat /pregel+_data/toy_output/part_0_0

# time mpiexec -n 32 ./run /pregel+_data/toy_graph.txt
time mpiexec -n 32 ./run /pregel+_data/pregel+-adj-8-Diameter.txt

# time mpiexec -n 32 ./run /pregel+_data/pregel+-adj-3600000
# time mpiexec -n 32 ./run /pregel+_data/pregel+-adj-8-Diameter_
# time mpiexec -n 32 ./run /pregel+_data/pregel+-adj-8-Density