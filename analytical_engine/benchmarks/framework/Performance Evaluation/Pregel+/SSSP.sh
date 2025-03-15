scale=8

for dataset in Standard Density Diameter; do
    for threads in 1 2 4 8 16 32; do
        machines=1
        echo "sssp $dataset   $machines machines   $threads threads    $((machines*threads)) total progresses"
        mpiexec --mca btl ^openib --mca btl_tcp_if_include bond0 -np $threads ./run /pregel+_data/pregel+-adj-${scale}-${dataset}.txt  \
            2>&1 \
            | awk '{ print strftime("[%Y-%m-%d %H:%M:%S]"), $0; fflush(); }' \
            > log_${dataset}_${machines}machines_${threads}threads.txt
    done
done

for ((id = 0;id <= 15; id++)); do
    echo "GraphExperiment${id} slots=${threads}" | tee -a host_file
done
for slave in GraphExperiment0 GraphExperiment1 GraphExperiment2 GraphExperiment3 GraphExperiment4 GraphExperiment5 GraphExperiment6 GraphExperiment7 GraphExperiment8 GraphExperiment9 GraphExperiment10 GraphExperiment11 GraphExperiment12 GraphExperiment13 GraphExperiment14 GraphExperiment15; do   
    scp -r /home/admin/pregel+/host_file admin@$slave:/home/admin/pregel+/
done

for dataset in Standard Density Diameter; do
    for machines in 1 2 4 8 16; do
        threads=32

        echo "sssp $dataset   $machines machines   $threads threads    $((machines*threads)) total progresses"
        mpirun --mca btl ^openib --mca btl_tcp_if_include bond0 -np $((machines*threads)) -hostfile /home/admin/pregel+/host_file ./run /pregel+_data/pregel+-adj-${scale}-${dataset}.txt  \
            2>&1 \
            | awk '{ print strftime("[%Y-%m-%d %H:%M:%S]"), $0; fflush(); }' \
            > log_${dataset}_${machines}machines_${threads}threads.txt
    done
done
