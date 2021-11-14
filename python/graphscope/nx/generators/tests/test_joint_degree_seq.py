import pytest
from networkx.algorithms.assortativity import degree_mixing_dict

# fmt: off
from networkx.generators.tests.test_joint_degree_seq import \
    test_is_valid_directed_joint_degree
from networkx.generators.tests.test_joint_degree_seq import test_is_valid_joint_degree
from networkx.generators.tests.test_joint_degree_seq import test_joint_degree_graph

from graphscope.nx.generators import gnm_random_graph
from graphscope.nx.generators import powerlaw_cluster_graph
from graphscope.nx.generators.joint_degree_seq import directed_joint_degree_graph
from graphscope.nx.generators.joint_degree_seq import is_valid_directed_joint_degree
from graphscope.nx.generators.joint_degree_seq import is_valid_joint_degree
from graphscope.nx.generators.joint_degree_seq import joint_degree_graph
from graphscope.nx.utils.compat import with_graphscope_nx_context


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(test_is_valid_joint_degree)
def test_is_valid_joint_degree():
    pass


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(test_joint_degree_graph)
def test_joint_degree_graph():
    pass


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(test_is_valid_directed_joint_degree)
def test_is_valid_directed_joint_degree():
    pass
