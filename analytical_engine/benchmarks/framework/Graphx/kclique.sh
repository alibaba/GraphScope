SPARK_MASTER=$1
DATA_DIR=$2

hdfs dfs -test -d /graphx_data
if [ $? -ne 0 ]; then
  hdfs dfs -mkdir -p /graphx_data
fi

for dataset in Standard Diameter Density; do
    hdfs dfs -rm -f /graphx_data/input.txt
    hdfs dfs -put ${DATA_DIR}/graphx-weight-edges-8-${dataset}.txt /graphx_data/input.txt
    hdfs dfs -ls /graphx_data

    for machines in 16 8 4 2 1; do
    threads=32
    tot_core=$((machines*threads))
    $SPARK_HOME/bin/spark-submit \
        --class TriangleCountingExample \
        --master "${SPARK_MASTER}" \
        --conf spark.executor.instances=1 \
        --conf spark.executor.memory=480G \
        --total-executor-cores $tot_core \
        --conf spark.executor.cores=$threads \
        --driver-memory 480G \
        trianglecountingexample_2.11-0.1.jar $tot_core \
        > kc${dataset}-8-${machines}machines-${threads}threads.log
    done
done