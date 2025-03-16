for dataset in Standard Diameter Density; do
    hdfs dfs -rm /graphx_data/input.txt
    hdfs dfs -cp /graphx_data/graphx-weight-edges-7-${dataset}.txt /graphx_data/input.txt
    hdfs dfs -ls /graphx_data

    machines=1
    threads=32
    tot_core=$((machines*threads))
    $SPARK_HOME/bin/spark-submit \
        --class TriangleCountingExample \
        --master spark://GraphK8sMaster:7077 \
        --conf spark.executor.instances=1 \
        --conf spark.executor.memory=480G \
        --total-executor-cores $tot_core \
        --conf spark.executor.cores=$threads \
        --driver-memory 480G \
        trianglecountingexample_2.11-0.1.jar $tot_core \
        > a_${dataset}-8-${machines}machines-${threads}threads.txt
done