import graphscope
graphscope.set_option(show_log=True)
sess = graphscope.session(cluster_type="hosts", num_workers=1)
grape_demo="/home/graphscope/GraphScope/analytical_engine/java/grape-demo/target/grape-demo-0.1-shaded.jar"
sess.add_lib(grape_demo)
graph = sess.load_from(vertices = "/home/graphscope/data/gstest/p2p-31.v", vformat="giraph:com.alibaba.graphscope.samples.format.P2PVertexInputFormat",\
     edges="/home/graphscope/data/gstest/p2p-31.e", eformat="giraph:com.alibaba.graphscope.samples.format.P2PEdgeInputFormat")


./giraph_runner --vertex_input_format_class  giraph:com.alibaba.graphscope.samples.format.P2PVertexInputFormat \
--edge_input_format_class giraph:com.alibaba.graphscope.samples.format.P2PEdgeInputFormat \
--vfile ~/data/gstest/p2p-31.v  --efile ~/data/gstest/p2p-31.e