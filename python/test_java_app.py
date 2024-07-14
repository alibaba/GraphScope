import graphscope
import os
from graphscope.framework.app import load_app
from graphscope.framework.loader import Loader

graphscope.set_option(show_log=True)
graphscope.set_option(log_level="DEBUG")

# sess = graphscope.session(cluster_type="k8s", enabled_engines="gae-java",k8s_image_tag="0.27.0") # k8s_volumes=k8s_volumes, 
# sess.add_lib('/mnt/zhanglei/code/giraph-dev/GraphScope/analytical_engine/java/grape-demo/target/grape-demo-0.27.0-shaded.jar')

sess = graphscope.session(cluster_type="hosts", num_workers=1, enabled_engines="gae-java")
sess.add_lib('/workspaces/GraphScope/analytical_engine/java/grape-demo/target/grape-demo-0.27.0-shaded.jar')


#vloader = Loader(source="hdfs:///test/person.csv",host="host.minikube.internal",port=9000,delimiter='|')
#eloader = Loader(source="hdfs:///test/knows.csv",host="host.minikube.internal",port=9000,delimiter='|')
#graph2 = sess.load_from(vertices=vloader, edges=eloader)
graph2 = sess.load_from(vertices=Loader(source="/workspaces/GraphScope/analytical_engine/test/modern_graph/person.csv",delimiter='|'),
                           edges=Loader(source="/workspaces/GraphScope/analytical_engine/test/modern_graph/knows.csv", delimiter='|'))
proj_g2 = graph2._project_to_simple(v_prop="age", e_prop="weight")

user_app = load_app(algo="java_pie:com.alibaba.graphscope.example.circle.CirclePIE")
res = user_app(proj_g2)