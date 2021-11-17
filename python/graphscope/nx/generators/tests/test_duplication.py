"""Unit tests for the :mod:`networkx.generators.duplication` module.

"""
import pytest
from networkx.generators.tests.test_duplication import TestDuplicationDivergenceGraph
from networkx.generators.tests.test_duplication import TestPartialDuplicationGraph

from graphscope.nx.generators.duplication import duplication_divergence_graph
from graphscope.nx.generators.duplication import partial_duplication_graph
from graphscope.nx.utils.compat import with_graphscope_nx_context


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestDuplicationDivergenceGraph)
class TestDuplicationDivergenceGraph:
    pass


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestPartialDuplicationGraph)
class TestPartialDuplicationGraph:
    pass
