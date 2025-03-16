#Replace '/apsara/GraphBenchmark/',                 \
    '/apsara/GraphBenchmark/flash/scratch/gfs/' and \
        '/home/admin/' with the actual paths.

scale=8

for dataset in Standard Density Diameter;
do
    for
      threads in 1 2 4 8 16 32;
do
  machines = 1 echo "SSSP $dataset   $machines machines   $threads threads    "
                    "$((machines*threads)) total progresses"./
                     format.sh $threads / apsara / GraphBenchmark / flash -
                 edges - ${scale} -
                 ${dataset} / / apsara / GraphBenchmark / flash / scratch /
                     gfs / flash -
                 sssp - edges - ${scale} - ${dataset} mpirun-- mca btl ^
             openib-- mca btl_tcp_if_include bond0 -
                 np $threads./ SSSP / apsara / GraphBenchmark / flash /
                     scratch / gfs / flash -
                 sssp - edges - ${scale} - $ {
    dataset
  }
0 2 > &1 |
    awk '{ print strftime("[%Y-%m-%d %H:%M:%S]"), $0; fflush(); }' >
        log_${dataset} _${machines} machines_${threads} threads.txt done done

        for ((id = 0; id <= 15; id++));
do
    echo "GraphExperiment${id} slots=${threads}" | tee -a host_file
done
for slave in GraphExperiment0 GraphExperiment1 GraphExperiment2 GraphExperiment3 GraphExperiment4 GraphExperiment5 GraphExperiment6 GraphExperiment7 GraphExperiment8 GraphExperiment9 GraphExperiment10 GraphExperiment11 GraphExperiment12 GraphExperiment13 GraphExperiment14 GraphExperiment15;
do
    scp -r /home/admin/flash/run/host_file admin@$slave:/home/admin/flash/run/
done

for dataset in Standard Density Diameter;
do
    for
      machines in 1 2 4 8 16;
do
  threads =
      32

              echo
              "SSSP $dataset   $machines machines   $threads threads    "
              "$((machines*threads)) total progresses"./
              format1.sh $((machines * threads)) / apsara / GraphBenchmark /
              flash -
          edges - ${scale} -
          ${dataset} / / apsara / GraphBenchmark / flash / scratch / gfs /
              flash -
          sssp - edges - ${scale} -
          ${dataset} / home / admin / flash / run / host_file mpirun-- bind -
          to core-- mca btl ^
      openib-- mca btl_tcp_if_include bond0 - np $((machines * threads)) -
          hostfile / home / admin / flash / run / host_file./ SSSP / apsara /
              GraphBenchmark / flash / scratch / gfs / flash -
          sssp - edges - ${scale} - $ {
    dataset
  }
0 2 > &1 |
    awk '{ print strftime("[%Y-%m-%d %H:%M:%S]"), $0; fflush(); }' >
        log_${dataset} _${machines} machines_${threads} threads.txt done done
