for dataset in Standard; do
    scale=1

    hdfs dfs -rm /graphx_data/input.txt
    hdfs dfs -cp /graphx_data/graphx-weight-edges-${scale}-${dataset}.txt /graphx_data/input.txt
    hdfs dfs -ls /graphx_data

    for machines in 1; do
    threads=32
    tot_core=$((machines*threads))

    $SPARK_HOME/bin/spark-submit \
        --class SSSPExample \
        --master spark://GraphK8sMaster:7077 \
        --conf spark.executor.instances=1 \
        --conf spark.executor.memory=480G \
        --total-executor-cores $tot_core \
        --conf spark.executor.cores=$threads \
        --driver-memory 480G \
        ssspexample_2.11-0.1.jar $tot_core \
        > a_${dataset}-${scale}-${machines}machines-${threads}threads.txt
    done
done