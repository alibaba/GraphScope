import networkx.algorithms.tests.test_simple_paths
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx
from graphscope.nx.utils.compat import with_graphscope_nx_context

import_as_graphscope_nx(networkx.algorithms.tests.test_simple_paths,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

from networkx.algorithms.tests.test_simple_paths import TestIsSimplePath


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestIsSimplePath)
class TestIsSimplePath:
    @pytest.mark.skip(reason="not support multigraph")
    def test_multigraph(self):
        pass

    @pytest.mark.skip(reason="not support multigraph")
    def test_multidigraph(self):
        pass


@pytest.mark.skip(reason="not support multigraph")
def test_all_simple_paths_multigraph(self):
    pass


@pytest.mark.skip(reason="not support multigraph")
def test_all_simple_paths_multigraph_with_cutoff(self):
    pass


@pytest.mark.skip(reason="not support multigraph")
def test_ssp_multigraph(self):
    pass


def test_cutoff_zero():
    G = nx.complete_graph(4)
    paths = nx.all_simple_paths(G, 0, 3, cutoff=0)
    assert list(list(p) for p in paths) == []


def test_source_missing():
    with pytest.raises(nx.NodeNotFound):
        G = nx.Graph()
        nx.add_path(G, [1, 2, 3])
        paths = list(nx.all_simple_paths(G, 0, 3))


def test_target_missing():
    with pytest.raises(nx.NodeNotFound):
        G = nx.Graph()
        nx.add_path(G, [1, 2, 3])
        paths = list(nx.all_simple_paths(G, 1, 4))
