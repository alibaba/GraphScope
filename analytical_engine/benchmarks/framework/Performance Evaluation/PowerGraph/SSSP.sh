scale=8

for dataset in Standard Density Diameter; do
    for threads in 1 2 4 8 16 32; do
        machines=1
        echo "SSSP $dataset   $machines machines   $threads threads    $((machines*threads)) total progresses"
        mpiexec -n $((machines*threads)) --bind-to core --mca btl_tcp_if_include bond0 \
            /home/admin/graphlab/release/toolkits/graph_analytics/sssp --source=0 --format=snap \
            /apsara/GraphBenchmarkDatasets/powergraph-edges-${scale}-${dataset}.txt \
            2>&1 \
            | awk '{ print strftime("[%Y-%m-%d %H:%M:%S]"), $0; fflush(); }' \
            > log_${dataset}_${machines}machines_${threads}threads.txt
    done
done

for ((id = 0;id <= 15; id++)); do
    echo "GraphExperiment${id} slots=${threads}" | tee -a ../machines.txt
done
for slave in GraphExperiment0 GraphExperiment1 GraphExperiment2 GraphExperiment3 GraphExperiment4 GraphExperiment5 GraphExperiment6 GraphExperiment7 GraphExperiment8 GraphExperiment9 GraphExperiment10 GraphExperiment11 GraphExperiment12 GraphExperiment13 GraphExperiment14 GraphExperiment15; do   
    scp -r /home/admin/graphlab/machines.txt admin@$slave:/home/admin/graphlab/
done

for dataset in Standard Density Diameter; do
    for machines in 1 2 4 8 16; do
        threads=32

        echo "SSSP $dataset   $machines machines   $threads threads    $((machines*threads)) total progresses"
        mpiexec -n $((machines*threads)) --bind-to core --mca btl_tcp_if_include bond0 --hostfile ../machines.txt \
            /home/admin/graphlab/release/toolkits/graph_analytics/sssp --source=0 --format=snap \
            /apsara/GraphBenchmarkDatasets/powergraph-edges-${scale}-${dataset}.txt \
            2>&1 \
            | awk '{ print strftime("[%Y-%m-%d %H:%M:%S]"), $0; fflush(); }' \
            > log_${dataset}_${machines}machines_${threads}threads.txt
    done
done
