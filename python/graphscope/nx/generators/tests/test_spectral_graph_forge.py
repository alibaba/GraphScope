import pytest

# fmt: off
from networkx.generators.tests.test_spectral_graph_forge import \
    test_spectral_graph_forge

from graphscope.nx import NetworkXError
from graphscope.nx import is_isomorphic
from graphscope.nx.generators import karate_club_graph
from graphscope.nx.generators.spectral_graph_forge import spectral_graph_forge
from graphscope.nx.tests.utils import assert_nodes_equal
from graphscope.nx.utils.compat import with_graphscope_nx_context

# fmt: on


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(test_spectral_graph_forge)
def test_spectral_graph_forge():
    pass
