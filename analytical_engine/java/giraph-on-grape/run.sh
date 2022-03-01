
export MAX_SUPER_STEP=10
#export MESSAGE_MANAGER_TYPE=netty
export MESSAGE_MANAGER_TYPE=mpi
export USER_JAR_PATH=/home/admin/gs/analytical_engine/java/giraph-on-grape/target/giraph-on-grape-shaded.jar
export OUT_MESSAGE_CACHE_TYPE=ByteBuf
export MESSAGE_STORE_TYPE=primitive
source /opt/graphscope/conf/grape_jvm_opts
GLOG_v=10 mpirun \
-envlist GLOG_v,GRAPE_JVM_OPTS,USER_JAR_PATH,GRAPH_TYPE,APP_TYPE,MAX_SUPER_STEP,MESSAGE_MANAGER_TYPE \
-n 4  ./giraph_runner --app_class com.alibaba.graphscope.samples.MessageBenchMark  \
--efile ~/libgrape-lite/dataset/p2p-31.e --vfile ~/libgrape-lite/dataset/p2p-31.v \
--worker_context_class com.alibaba.graphscope.samples.MessageBenchMark\$MessageBenchMarkWorkerContext \
--lib_path /opt/graphscope/lib/libgiraph-jni.so --loading_thread_num 1 \
--serialize true --serialize_prefix p2p

# datagen
GLOG_v=2 mpirun \
-n 4 \
-envlist MESSAGE_STORE_TYPE,GLOG_v,GRAPE_JVM_OPTS,USER_JAR_PATH,GRAPH_TYPE,APP_TYPE,MAX_SUPER_STEP,MESSAGE_MANAGER_TYPE,OUT_MESSAGE_CACHE_TYPE,MESSAGE_STORE_TYPE \
./giraph_runner --app_class com.alibaba.graphscope.samples.MessageBenchMark \
--efile ~/libgrape-lite/dataset/p2p-31.e --vfile ~/libgrape-lite/dataset/p2p-31.v \
--worker_context_class com.alibaba.graphscope.samples.MessageBenchMark\$MessageBenchMarkWorkerContext \
--lib_path /opt/graphscope/lib/libgiraph-jni.so --loading_thread_num 1 \
--serialize true --serialize_prefix p2p --grape_loader true


--efile ./lei.e --vfile ./lei.v \
# com 
GLOG_v=10 mpirun \
-n 4 \
-f ~/hostfile \
-envlist GLOG_v,GRAPE_JVM_OPTS,USER_JAR_PATH,GRAPH_TYPE,APP_TYPE,MAX_SUPER_STEP,MESSAGE_MANAGER_TYPE,OUT_MESSAGE_CACHE_TYPE,MESSAGE_STORE_TYPE \
./giraph_runner --app_class com.alibaba.graphscope.samples.MessageBenchMark \
--efile ~/com.e --vfile ~/com.v \
--worker_context_class com.alibaba.graphscope.samples.MessageBenchMark\$MessageBenchMarkWorkerContext \
--lib_path /opt/graphscope/lib/libgiraph-jni.so --loading_thread_num 1 \
--serialize true --serialize_prefix com --grape_loader true

GLOG_v=10 mpirun \
-n 2 \
-f ~/hostfile \
-envlist GLOG_v,GRAPE_JVM_OPTS,USER_JAR_PATH,GRAPH_TYPE,APP_TYPE,MAX_SUPER_STEP,MESSAGE_MANAGER_TYPE,OUT_MESSAGE_CACHE_TYPE,MESSAGE_STORE_TYPE \
./giraph_runner --app_class com.alibaba.graphscope.samples.MessageBenchMark \
--efile ~/com.e --vfile ~/com.v \
--worker_context_class com.alibaba.graphscope.samples.MessageBenchMark\$MessageBenchMarkWorkerContext \
--lib_path /opt/graphscope/lib/libgiraph-jni.so --loading_thread_num 1 \
--serialize true --serialize_prefix com --grape_loader true

GLOG_v=10 mpirun \
-n 4 \
-f ~/hostfile \
-envlist GLOG_v,GRAPE_JVM_OPTS,USER_JAR_PATH,GRAPH_TYPE,APP_TYPE,MAX_SUPER_STEP,MESSAGE_MANAGER_TYPE,OUT_MESSAGE_CACHE_TYPE,MESSAGE_STORE_TYPE \
./giraph_runner --app_class com.alibaba.graphscope.samples.MessageBenchMark \
--efile ~/libgrape-lite/dataset/p2p-31.e --vfile ~/libgrape-lite/dataset/p2p-31.v \
--worker_context_class com.alibaba.graphscope.samples.MessageBenchMark\$MessageBenchMarkWorkerContext \
--lib_path /opt/graphscope/lib/libgiraph-jni.so --loading_thread_num 1 \
--serialize true --serialize_prefix p2p \
--grape_loader true



osscmd multiget --thread_num=16 oss://lei-benchdata/datagen-9_0-fb.v datagen.v

nohup osscmd multiget --thread_num=16 oss://grape-data/com-friendster/evr/com-friendster.v com.v && nohup osscmd multiget --thread_num=16 oss://grape-data/com-friendster/evr/com-friendster.e com.e &

GLOG_v=10 mpirun -envlist GLOG_v,GRAPE_JVM_OPTS,USER_JAR_PATH,GRAPH_TYPE,APP_TYPE,MAX_SUPER_STEP,MESSAGE_MANAGER_TYPE -n 4 ./giraph_runner --app_class com.alibaba.graphscope.samples.MessageBenchMark  --efile ~/libgrape-lite/dataset/p2p-31.e --vfile ~/libgrape-lite/dataset/p2p-31.v --worker_context_class com.alibaba.graphscope.samples.MessageBenchMark\$MessageBenchMarkWorkerContext --lib_path /opt/graphscope/lib/libgiraph-jni.so --loading_thread_num 1
# --efile ./datagen-9_0.e --vfile ./datagen-9_0.v \

I0119 19:44:51.927856  4862 message_bench.h:83] Frag [1] send msg size: 36559 in step: 8
I0119 19:44:51.927870  4861 message_bench.h:83] Frag [0] send msg size: 37340 in step: 8
I0119 19:44:51.927908  4863 message_bench.h:83] Frag [2] send msg size: 37125 in step: 8
I0119 19:44:51.927927  4864 message_bench.h:83] Frag [3] send msg size: 36868 in step: 8

./giraph_runner --efile ~/libgrape-lite/dataset/p2p-31.e \
--vfile ~/libgrape-lite/dataset/p2p-31.v \
--lib_path /opt/graphscope/lib/libgiraph-jni.so --loading_thread_num 1 \
--app_class com.alibaba.graphscope.samples.MessageBenchMark \
--lib_path /opt/graphscope/lib/libgiraph-jni.so --loading_thread_num 1 \
--vertex_input_format_class com.alibaba.graphscope.samples.format.P2PVertexInputFormat \
--edge_input_format_class com.alibaba.graphscope.samples.format.P2PEdgeInputFormat


GLOG_v=10 ./giraph_runner --efile ~/libgrape-lite/dataset/p2p-31.e \
--vfile ~/libgrape-lite/dataset/p2p-31.v --lib_path /opt/graphscope/lib/libgiraph-jni.so \
--loading_thread_num 1 --user_app_class com.alibaba.graphscope.samples.MessageBenchMark \
--lib_path /opt/graphscope/lib/libgiraph-jni.so --loading_thread_num 1 \
--vertex_input_format_class com.alibaba.graphscope.samples.format.P2PVertexInputFormat \
--edge_input_format_class com.alibaba.graphscope.samples.format.P2PEdgeInputFormat \
--message_combiner_class org.apache.giraph.combiner.MinMessageCombiner

## ssspLLLL
GLOG_v=2 ./giraph_runner --efile ~/libgrape-lite/dataset/p2p-31.e \
--vfile ~/libgrape-lite/dataset/p2p-31.v --lib_path /opt/graphscope/lib/libgiraph-jni.so \
--loading_thread_num 1 --user_app_class com.alibaba.graphscope.samples.SSSPLLLL \
--worker_context_class com.alibaba.graphscope.samples.SSSP\$SSSPWorkerContext \
--lib_path /opt/graphscope/lib/libgiraph-jni.so --loading_thread_num 1 \
--vertex_input_format_class com.alibaba.graphscope.samples.format.P2PVertexInputFormat \
--edge_input_format_class com.alibaba.graphscope.samples.format.P2PEdgeInputFormat \
--message_combiner_class org.apache.giraph.combiner.LongLongMinMessageCombiner

## ssspLLLL -livejournal
GLOG_v=2 ./giraph_runner --efile ~/livejournal.e.csv \
--vfile ~/livejournal.v.csv --lib_path /opt/graphscope/lib/libgiraph-jni.so \
--loading_thread_num 1 --user_app_class com.alibaba.graphscope.samples.SSSPLLLL \
--worker_context_class com.alibaba.graphscope.samples.SSSP\$SSSPWorkerContext \
--lib_path /opt/graphscope/lib/libgiraph-jni.so --loading_thread_num 1 \
--vertex_input_format_class com.alibaba.graphscope.samples.format.LiveJournalVertexInputFormat \
--edge_input_format_class com.alibaba.graphscope.samples.format.LiveJournalEdgeInputFormat \
--message_combiner_class org.apache.giraph.combiner.LongLongMinMessageCombiner 

env GLOG_v,GRAPE_JVM_OPTS,USER_JAR_PATH,,MESSAGE_MANAGER_TYPE,OUT_MESSAGE_CACHE_TYPE,MESSAGE_STORE_TYPE,SSSP_SOURCE,BFS_SOURCE \
GLOG_v=2 mpirun -n 2 -hostfile ~/hostfile \
./giraph_runner --efile ~/p2p-31.e \
--vfile ~/p2p-31.v --lib_path /opt/graphscope/lib/libgiraph-jni.so \
--loading_thread_num 1 --user_app_class com.alibaba.graphscope.samples.SSSPLLLL \
--worker_context_class com.alibaba.graphscope.samples.SSSP\$SSSPWorkerContext \
--lib_path /opt/graphscope/lib/libgiraph-jni.so --loading_thread_num 1 \
--vertex_input_format_class com.alibaba.graphscope.samples.format.P2PVertexInputFormat \
--edge_input_format_class com.alibaba.graphscope.samples.format.P2PEdgeInputFormat \
--message_combiner_class org.apache.giraph.combiner.LongLongMinMessageCombiner 





# bfs
GLOG_v=10 ./giraph_runner --efile ~/libgrape-lite/dataset/p2p-31.e \
--vfile ~/libgrape-lite/dataset/p2p-31.v --lib_path /opt/graphscope/lib/libgiraph-jni.so \
--loading_thread_num 1 --user_app_class com.alibaba.graphscope.samples.BFSLLLL \
--lib_path /opt/graphscope/lib/libgiraph-jni.so --loading_thread_num 1 \
--vertex_input_format_class com.alibaba.graphscope.samples.format.P2PVertexInputFormat \
--edge_input_format_class com.alibaba.graphscope.samples.format.P2PEdgeInputFormat \
--message_combiner_class org.apache.giraph.combiner.LongLongMinMessageCombiner

#bfs lei
GLOG_v=10 ./giraph_runner --efile ./lei.e \
--vfile ./lei.v --lib_path /opt/graphscope/lib/libgiraph-jni.so \
--loading_thread_num 1 --user_app_class com.alibaba.graphscope.samples.BFSLLLL \
--lib_path /opt/graphscope/lib/libgiraph-jni.so --loading_thread_num 1 \
--vertex_input_format_class com.alibaba.graphscope.samples.format.P2PVertexInputFormat \
--edge_input_format_class com.alibaba.graphscope.samples.format.P2PEdgeInputFormat \
--message_combiner_class org.apache.giraph.combiner.LongLongMinMessageCombiner

import graphscope
graphscope.set_option(show_log=True)
sess = graphscope.session(cluster_type="hosts", num_workers=1)
graph = sess.g()
graph = graph.add_vertices("/tmp/gstest/property/p2p-31_property_v_0", "host").add_edges("/tmp/gstest/property/p2p-31_property_e_0","connect",src_label="host",dst_label="host")


hadoop jar /home/admin/xiaolei/giraph/giraph-examples/target/giraph-examples-1.4.0-SNAPSHOT-for-hadoop-2.5.1-jar-with-dependencies.jar \
 org.apache.giraph.GiraphRunner -Dmapred.job.queue.name=product org.apache.giraph.examples.SimpleShortestPathsComputation  \
 -vif org.apache.giraph.io.formats.JsonLongDoubleFloatDoubleVertexInputFormat \
 -vip /giraph/data/tiny_graph.txt -vof org.apache.giraph.io.formats.IdWithValueTextOutputFormat \
 -op /giraph/data/shortestpaths -w 1

 hadoop jar /home/admin/xiaolei/giraph/giraph-examples/target/giraph-examples-1.4.0-SNAPSHOT-for-hadoop-2.5.1-jar-with-dependencies.jar \
 org.apache.giraph.GiraphRunner -Dmapred.job.queue.name=product \
 -Dmapreduce.map.memory.mb=16384 \
 -Dmapred.map.java.opts=-Xmx16384m \
 -Dmapred.child.java.opts=-Xmx16384m \
  org.apache.giraph.examples.SimpleShortestPathsComputation  \
 -vif org.apache.giraph.examples.io.formats.LiveJournalVertexInputFormat \
 -eif org.apache.giraph.examples.io.formats.LiveJournalEdgeInputFormat \
 -vip /giraph/input/livejournal.v.csv \
 -eip /giraph/input/livejournal.e.csv \
 -vof org.apache.giraph.io.formats.IdWithValueTextOutputFormat \
 -op /giraph/output/livejounal-sssp -w 1


# run sssp on p2p
GLOG_v,GRAPE_JVM_OPTS,USER_JAR_PATH,,MESSAGE_MANAGER_TYPE,OUT_MESSAGE_CACHE_TYPE,MESSAGE_STORE_TYPE,SSSP_SOURCE,BFS_SOURCE
GLOG_v=2 mpiexec --mca btl_tcp_if_include bond0 --host d50,d51,d52,d53 -np 4 -x GLOG_v -x GRAPE_JVM_OPTS -x USER_JAR_PATH -x MESSAGE_MANAGER_TYPE \
-x OUT_MESSAGE_CACHE_TYPE -x MESSAGE_STORE_TYPE -x SSSP_SOURCE -x LOADING_THREAD_NUM \
-x BFS_SOURCE ./giraph_runner --efile ~/data/gstest/p2p-31.e \
--query_times 5 \
--vfile ~/data/gstest/p2p-31.v --lib_path /opt/graphscope/lib/libgiraph-jni.so \
--loading_thread_num 1 --user_app_class com.alibaba.graphscope.samples.SSSPLLLL \
--worker_context_class com.alibaba.graphscope.samples.SSSP\$SSSPWorkerContext \
--vertex_input_format_class com.alibaba.graphscope.samples.format.P2PVertexInputFormat \
--edge_input_format_class com.alibaba.graphscope.samples.format.P2PEdgeInputFormat \
--message_combiner_class org.apache.giraph.combiner.LongLongMinMessageCombiner

#run sssp on livejournal
GLOG_v=2 mpiexec --mca btl_tcp_if_include bond0 --host d50,d51,d52,d53 -np 4 -x GLOG_v -x GRAPE_JVM_OPTS -x USER_JAR_PATH -x MESSAGE_MANAGER_TYPE \
-x OUT_MESSAGE_CACHE_TYPE -x MESSAGE_STORE_TYPE -x SSSP_SOURCE -x LOADING_THREAD_NUM \
-x BFS_SOURCE ./giraph_runner --efile ~/data/livejournal.e \
--query_times 5 \
--vfile ~/data/livejournal.v --lib_path /opt/graphscope/lib/libgiraph-jni.so \
--loading_thread_num 1 --user_app_class com.alibaba.graphscope.samples.SSSPLLLL \
--worker_context_class com.alibaba.graphscope.samples.SSSP\$SSSPWorkerContext \
--vertex_input_format_class com.alibaba.graphscope.samples.format.LiveJournalVertexInputFormat \
--edge_input_format_class com.alibaba.graphscope.samples.format.LiveJournalEdgeInputFormat \
--message_combiner_class org.apache.giraph.combiner.LongLongMinMessageCombiner

# run message app on p2p
GLOG_v,GRAPE_JVM_OPTS,USER_JAR_PATH,,MESSAGE_MANAGER_TYPE,OUT_MESSAGE_CACHE_TYPE,MESSAGE_STORE_TYPE,SSSP_SOURCE,BFS_SOURCE
GLOG_v=1 mpiexec --mca btl_tcp_if_include bond0 --host d50,d51,d52,d53 -np 4 -x GLOG_v -x GRAPE_JVM_OPTS -x USER_JAR_PATH -x MESSAGE_MANAGER_TYPE \
-x OUT_MESSAGE_CACHE_TYPE -x MESSAGE_STORE_TYPE -x SSSP_SOURCE -x LOADING_THREAD_NUM -x MAX_SUPER_STEP \
-x BFS_SOURCE ./giraph_runner --efile ~/data/gstest/p2p-31.e \
--query_times 5 \
--vfile ~/data/gstest/p2p-31.v --lib_path /opt/graphscope/lib/libgiraph-jni.so \
--loading_thread_num 1 --user_app_class com.alibaba.graphscope.samples.MessageBenchMark \
--worker_context_class com.alibaba.graphscope.samples.MessageBenchMark\$MessageBenchMarkWorkerContext \
--vertex_input_format_class com.alibaba.graphscope.samples.format.P2PVertexInputFormat \
--edge_input_format_class com.alibaba.graphscope.samples.format.P2PEdgeInputFormat \
--message_combiner_class org.apache.giraph.combiner.LongLongMinMessageCombiner \
--query_times 1

# run msg app on livejournal
GLOG_v=2 mpiexec --mca btl_tcp_if_include bond0 --host d50,d51,d52,d53 -np 4 -x GLOG_v -x GRAPE_JVM_OPTS -x USER_JAR_PATH -x MESSAGE_MANAGER_TYPE \
-x OUT_MESSAGE_CACHE_TYPE -x MESSAGE_STORE_TYPE -x SSSP_SOURCE -x LOADING_THREAD_NUM -x MAX_SUPER_STEP \
-x BFS_SOURCE ./giraph_runner --efile ~/data/livejournal.e \
--vfile ~/data/livejournal.v --lib_path /opt/graphscope/lib/libgiraph-jni.so \
--loading_thread_num 1 --user_app_class com.alibaba.graphscope.samples.MessageBenchMark \
--worker_context_class com.alibaba.graphscope.samples.MessageBenchMark\$MessageBenchMarkWorkerContext \
--vertex_input_format_class com.alibaba.graphscope.samples.format.LiveJournalVertexInputFormat \
--edge_input_format_class com.alibaba.graphscope.samples.format.LiveJournalEdgeInputFormat \
--message_combiner_class org.apache.giraph.combiner.LongLongMinMessageCombiner \
--query_times 1

# hadoop message bench on livejounal
 hadoop jar /home/admin/xiaolei/giraph/giraph-examples/target/giraph-examples-1.4.0-SNAPSHOT-for-hadoop-2.5.1-jar-with-dependencies.jar \
 org.apache.giraph.GiraphRunner -Dmapred.job.queue.name=product \
 -Dmapreduce.map.memory.mb=16384 \
 -Dmapred.map.java.opts=-Xmx16384m \
 -Dmapred.child.java.opts=-Xmx16384m \
  org.apache.giraph.examples.MessageBenchMark  \
 -vif org.apache.giraph.examples.io.formats.LiveJournalVertexInputFormat \
 -eif org.apache.giraph.examples.io.formats.LiveJournalEdgeInputFormat \
 -vip /giraph/input/livejournal.v.csv \
 -eip /giraph/input/livejournal.e.csv \
 -vof org.apache.giraph.io.formats.IdWithValueTextOutputFormat \
 -op /giraph/output/livejounal-message-bench -w 1

 #hadoop traverse on livejournal
  hadoop jar /home/admin/xiaolei/giraph/giraph-examples/target/giraph-examples-1.4.0-SNAPSHOT-for-hadoop-2.5.1-jar-with-dependencies.jar \
 org.apache.giraph.GiraphRunner -Dmapred.job.queue.name=product \
 -Dmapreduce.map.memory.mb=16384 \
 -Dmapred.map.java.opts=-Xmx16384m \
 -Dmapred.child.java.opts=-Xmx16384m \
  org.apache.giraph.examples.Traverse  \
 -vif org.apache.giraph.examples.io.formats.LiveJournalVertexInputFormat \
 -eif org.apache.giraph.examples.io.formats.LiveJournalEdgeInputFormat \
 -vip /giraph/input/livejournal.v.csv \
 -eip /giraph/input/livejournal.e.csv \
 -vof org.apache.giraph.io.formats.IdWithValueTextOutputFormat \
 -op /giraph/output/livejounal-traverse-bench -w 1 \
 -wc org.apache.giraph.examples.Traverse\$TraverseContext

 # pie traverse on livejournal
 GLOG_v=2 mpiexec --mca btl_tcp_if_include bond0 --host d50,d51,d52,d53 -np 2 -x GLOG_v -x GRAPE_JVM_OPTS -x USER_JAR_PATH -x MESSAGE_MANAGER_TYPE \
-x OUT_MESSAGE_CACHE_TYPE -x MESSAGE_STORE_TYPE -x SSSP_SOURCE -x LOADING_THREAD_NUM -x MAX_SUPER_STEP \
-x BFS_SOURCE ./giraph_runner --efile ~/data/livejournal.e \
--vfile ~/data/livejournal.v --lib_path /opt/graphscope/lib/libgiraph-jni.so \
--loading_thread_num 8 --user_app_class com.alibaba.graphscope.samples.Traverse \
--worker_context_class com.alibaba.graphscope.samples.Traverse\$TraverseContext \
--vertex_input_format_class com.alibaba.graphscope.samples.format.LiveJournalVertexInputFormat \
--edge_input_format_class com.alibaba.graphscope.samples.format.LiveJournalEdgeInputFormat \
--message_combiner_class org.apache.giraph.combiner.LongLongMinMessageCombiner \
--query_times 1 --edge_manager lazy 

#run traverse on simple graph.
 GLOG_v=2 mpiexec --mca btl_tcp_if_include bond0 --host d50,d51,d52,d53 -np 4 -x GLOG_v -x GRAPE_JVM_OPTS -x USER_JAR_PATH -x MESSAGE_MANAGER_TYPE \
-x OUT_MESSAGE_CACHE_TYPE -x MESSAGE_STORE_TYPE -x SSSP_SOURCE -x LOADING_THREAD_NUM -x MAX_SUPER_STEP \
-x BFS_SOURCE ./giraph_runner --efile ~/data/lei.e \
--vfile ~/data/lei.v --lib_path /opt/graphscope/lib/libgiraph-jni.so \
--loading_thread_num 1 --user_app_class com.alibaba.graphscope.samples.Traverse \
--worker_context_class com.alibaba.graphscope.samples.Traverse\$TraverseContext \
--vertex_input_format_class com.alibaba.graphscope.samples.format.P2PVertexInputFormat \
--edge_input_format_class com.alibaba.graphscope.samples.format.P2PEdgeInputFormat \
--message_combiner_class org.apache.giraph.combiner.LongLongMinMessageCombiner \
--query_times 1

#load graph
GLOG_v=2 mpiexec --mca btl_tcp_if_include bond0 --host d50,d51,d52,d53 -np 4 -x GLOG_v -x GRAPE_JVM_OPTS -x USER_JAR_PATH -x MAX_SUPER_STEP \
./load_giraph_fragment /tmp/vineyard.sock ~/data/gstest/p2p-31.v ~/data/gstest/p2p-31.e com.alibaba.graphscope.samples.format.P2PVertexInputFormat \
 com.alibaba.graphscope.samples.format.P2PEdgeInputFormat com.alibaba.graphscope.samples.MessageBenchMark  /opt/graphscope/lib/libgiraph-jni.so 1

#load graph livejournal
GLOG_v=2 mpiexec --mca btl_tcp_if_include bond0 --host d50,d51,d52,d53 -np 4 -x GLOG_v -x GRAPE_JVM_OPTS -x USER_JAR_PATH -x MAX_SUPER_STEP \
./load_giraph_fragment /tmp/vineyard.sock ~/data/livejournal.v ~/data/livejournal.e com.alibaba.graphscope.samples.format.LiveJournalVertexInputFormat \
 com.alibaba.graphscope.samples.format.LiveJournalEdgeInputFormat com.alibaba.graphscope.samples.Traverse /opt/graphscope/lib/libgiraph-jni.so 1

# run traverse with frag ids
  GLOG_v=2 mpiexec --mca btl_tcp_if_include bond0 --host d50,d51,d52,d53 -np 4 -x GLOG_v -x GRAPE_JVM_OPTS -x USER_JAR_PATH -x MESSAGE_MANAGER_TYPE \
-x OUT_MESSAGE_CACHE_TYPE -x MESSAGE_STORE_TYPE -x SSSP_SOURCE -x LOADING_THREAD_NUM -x MAX_SUPER_STEP \
-x BFS_SOURCE ./giraph_runner  --lib_path /opt/graphscope/lib/libgiraph-jni.so \
--frag_ids 226391108423929904,226393404684229300,226393899694124916,226405696511023304 \
--loading_thread_num 1 --user_app_class com.alibaba.graphscope.samples.Traverse \
--worker_context_class com.alibaba.graphscope.samples.Traverse\$TraverseContext \
--message_combiner_class org.apache.giraph.combiner.LongLongMinMessageCombiner \
--query_times 1 -edge_manager lazy 

226391108423929904,226393404684229300,226393899694124916,226405696511023304

GLOG_v=10 ./giraph_runner --vertex_input_format_class giraph:com.alibaba.graphscope.example.giraph.format.P2PVertexInputFormat \
--edge_input_format_class giraph:com.alibaba.graphscope.example.giraph.format.P2PEdgeInputFormat \
--efile ~/data/gstest/p2p-31.e --vfile  ~/data/gstest/p2p-31.v 
