import graphscope
from graphscope import JavaApp
from graphscope.dataset import load_p2p_network
from graphscope.framework.format import BuiltInFormats


sess = graphscope.session(cluster_type="hosts", num_workers=1)
graph = sess.g(directed=true)
graph = sess.load_from(vertices = "~/data/gstest/p2p-31.v", vformat=BuiltInFormats.)