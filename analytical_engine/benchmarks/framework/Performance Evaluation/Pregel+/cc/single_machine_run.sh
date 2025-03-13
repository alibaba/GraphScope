time mpiexec --mca btl ^openib --mca btl_tcp_if_include bond0 -np 32 ./run /pregel+_data/pregel+-adj-3600000
time mpiexec --mca btl ^openib --mca btl_tcp_if_include bond0 -np 32 ./run /pregel+_data/pregel+-adj-8-Diameter
time mpiexec --mca btl ^openib --mca btl_tcp_if_include bond0 -np 32 ./run /pregel+_data/pregel+-adj-8-Density