"""Unit tests for the :mod:`graphscope.nx.generators.expanders` module.

"""

# fmt: off
import pytest
from networkx import adjacency_matrix
from networkx.generators.tests.test_expanders import test_chordal_cycle_graph
from networkx.generators.tests.test_expanders import test_margulis_gabber_galil_graph
from networkx.generators.tests.test_expanders import \
    test_margulis_gabber_galil_graph_badinput

#fmt: off

try:
    from networkx.generators.tests.test_expanders import test_paley_graph
except ImportError:
    # NetworkX<=2.4 not contains paley_graph
    test_paley_graph = lambda: None


import graphscope.nx as nx
from graphscope.nx import number_of_nodes
from graphscope.nx.generators.expanders import chordal_cycle_graph
from graphscope.nx.generators.expanders import margulis_gabber_galil_graph
from graphscope.nx.utils.compat import with_graphscope_nx_context

try:
    from graphscope.nx.generators.expanders import paley_graph
except ImportError:
    # NetworkX <= 2.4
    pass


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(test_margulis_gabber_galil_graph)
def test_margulis_gabber_galil_graph():
    pass


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(test_chordal_cycle_graph)
def test_chordal_cycle_graph():
    pass


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(test_margulis_gabber_galil_graph_badinput)
def test_margulis_gabber_galil_graph_badinput():
    pass


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(test_paley_graph)
def test_paley_graph():
    pass
