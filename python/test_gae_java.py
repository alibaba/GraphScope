import graphscope
import os
from graphscope.framework.app import load_app

graphscope.set_option(show_log=True)
graphscope.set_option(log_level="DEBUG")
# k8s_volumes = {
#     "data": {
#         "type": "hostPath",
#         "field": {
#             "path": "/data",
#             "type": "Directory"
#         },
#         "mounts": {
#         "mountPath": "/data"
#         }
#     }
# }
sess = graphscope.session(cluster_type="k8s", enabled_engines="gae-java",k8s_image_tag="0.27.0") # k8s_volumes=k8s_volumes, 
sess.add_lib('/mnt/zhanglei/grape-demo-0.27.0-shaded.jar')
# sess.add_lib('/workspaces/GraphScope/analytical_engine/java/grape-demo/target/grape-demo-0.27.0-shaded.jar')
# vformat = "giraph:com.alibaba.graphscope.example.giraph.format.P2PVertexInputFormat"
# eformat = "giraph:com.alibaba.graphscope.example.giraph.format.P2PEdgeInputFormat"
# graph = sess.load_from(
#     vertices="/data/gstest/p2p-31.v",
#     vformat=vformat,
#     edges="/data/gstest/p2p-31.e",
#     eformat=eformat,
# )
# proj_g = graph._project_to_simple(v_prop="vdata", e_prop="data")
# giraph_sssp = load_app(algo="giraph:com.alibaba.graphscope.example.giraph.SSSP")
# res = giraph_sssp(proj_g, sourceId=6)

vformat2 = "giraph:com.alibaba.graphscope.example.giraph.format.P2PVertexMultipleLongInputFormat"
eformat2 = "giraph:com.alibaba.graphscope.example.giraph.format.P2PEdgeMultipleLongInputFormat"
# graph2 = sess.load_from(
#     vertices="/data/gstest/p2p-31.v",
#     vformat=vformat2,
#     edges="/data/gstest/p2p-31.e",
#     eformat=eformat2,
# )
graph2 = sess.load_from(
    vertices="hdfs://host.minikube.internal:9000/test/p2p-31.v",
    vformat=vformat2,
    edges="hdfs://host.minikube.internal:9000/test/p2p-31.e",
    eformat=eformat2,
)
proj_g2 = graph2._project_to_simple(v_prop="vdata", e_prop="data")

user_app = load_app(algo="giraph:com.alibaba.graphscope.example.giraph.MessageAppWithUserWritable")
res = user_app(proj_g2)