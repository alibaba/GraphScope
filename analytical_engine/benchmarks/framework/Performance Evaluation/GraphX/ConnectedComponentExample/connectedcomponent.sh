for dataset in Diameter Standard Density; do
    hdfs dfs -rm /graphx_data/input.txt
    hdfs dfs -cp /graphx_data/graphx-weight-edges-8-${dataset}.txt /graphx_data/input.txt
    hdfs dfs -ls /graphx_data

    machines=1
    threads=32
    tot_core=$((machines*threads))
    $SPARK_HOME/bin/spark-submit \
        --class ConnectedComponentExample \
        --master spark://GraphK8sMaster:7077 \
        --conf spark.executor.instances=1 \
        --conf spark.executor.memory=480G \
        --total-executor-cores $tot_core \
        --conf spark.executor.cores=$threads \
        --driver-memory 480G \
        connectedcomponentexample_2.11-0.1.jar $tot_core \
        > ${dataset}-8-${machines}machines-${threads}threads.txt
done