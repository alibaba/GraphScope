import graphscope
graphscope.set_option(show_log=True)
sess = graphscope.session(cluster_type="hosts", num_workers=1)
graph = sess.load_from(vertices = "~/data/gstest/p2p-31.v", vformat="giraph:com.alibaba.graphscope.samples.format.P2PVertexInputFormat",\
     edges="~/data/gstest/p2p-31.e", eformat="giraph:com.alibaba.graphscope.samples.format.P2PEdgeInputFormat")