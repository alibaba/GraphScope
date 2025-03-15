


make


for slave in GraphExperiment1 GraphExperiment2 GraphExperiment3 GraphExperiment4 GraphExperiment5 GraphExperiment6 GraphExperiment7 GraphExperiment8 GraphExperiment9 GraphExperiment10 GraphExperiment11 GraphExperiment12 GraphExperiment13 GraphExperiment14 GraphExperiment15; do   
    scp -r /home/sy429729/pregel+/cc admin@$slave:/home/admin/cc
    ssh admin@$slave 'sudo rm -rf /home/sy429729/pregel+/cc && sudo mv /home/admin/cc /home/sy429729/pregel+/ && sudo chmod 777 /home/sy429729/pregel+/*'; 
done


# echo "................................................................................................................当前循环次数: 2 machine 1 threads"
# time mpiexec --bind-to core --mca btl ^openib --mca btl_tcp_if_include bond0 -np 2 -hostfile ../pagerank/confs_threads_1/conf2 ./run

# echo "................................................................................................................当前循环次数: 4 machine 1 threads"
# time mpiexec --bind-to core --mca btl ^openib --mca btl_tcp_if_include bond0 -np 4 -hostfile ../pagerank/confs_threads_1/conf4 ./run

# echo "................................................................................................................当前循环次数: 8 machine 1 threads"
# time mpiexec --bind-to core --mca btl ^openib --mca btl_tcp_if_include bond0 -np 8 -hostfile ../pagerank/confs_threads_1/conf8 ./run

# echo "................................................................................................................当前循环次数: 16 machine 1 threads"
# time mpiexec --bind-to core --mca btl ^openib --mca btl_tcp_if_include bond0 -np 16 -hostfile ../pagerank/confs_threads_1/conf16 ./run

 

# echo "................................................................................................................当前循环次数: 2 machine 32 threads"
# time mpiexec --bind-to core --mca btl ^openib --mca btl_tcp_if_include bond0 -np 64 -hostfile ../pagerank/confs_threads_32/conf2 ./run /pregel+_data/pregel+-adj-3600000

# echo "................................................................................................................当前循环次数: 4 machine 32 threads"
# time mpiexec --bind-to core --mca btl ^openib --mca btl_tcp_if_include bond0 -np 128 -hostfile ../pagerank/confs_threads_32/conf4 ./run


# echo "................................................................................................................当前循环次数: 8 machine 32 threads"
# time mpiexec --bind-to core --mca btl ^openib --mca btl_tcp_if_include bond0 -np 256 -hostfile ../pagerank/confs_threads_32/conf8 ./run


echo "................................................................................................................当前循环次数: 16 machine 32 threads"
# time mpiexec --bind-to core --mca btl ^openib --mca btl_tcp_if_include bond0 -np 512 -hostfile ../pagerank/confs_threads_32/conf16 ./run /pregel+_data/pregel+-adj-3600000

# time mpiexec --bind-to core --mca btl ^openib --mca btl_tcp_if_include bond0 -np 512 -hostfile ../pagerank/confs_threads_32/conf16 ./run /pregel+_data/pregel+-adj-8-Diameter

# time mpiexec --bind-to core --mca btl ^openib --mca btl_tcp_if_include bond0 -np 512 -hostfile ../pagerank/confs_threads_32/conf16 ./run /pregel+_data/pregel+-adj-8-Density

time mpiexec --bind-to core --mca btl ^openib --mca btl_tcp_if_include bond0 -np 512 -hostfile ../pagerank/confs_threads_32/conf16 ./run /pregel+_data/pregel+-adj-10-Standard

