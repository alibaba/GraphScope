"""
====================
Generators - Classic
====================

Unit tests for various classic graph generators in generators/classic.py
"""
import networkx.generators.tests.test_classic
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx
from graphscope.nx.utils.compat import with_graphscope_nx_context

import_as_graphscope_nx(
    networkx.generators.tests.test_classic,
    decorators=pytest.mark.usefixtures("graphscope_session"),
)

from networkx.generators.tests.test_classic import TestGeneratorClassic

@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestGeneratorClassic)
class TestGeneratorClassic:
    @pytest.mark.skip(reason="FIXME: test take too much time.")
    def test_dorogovtsev_goltsev_mendes_graph(self):
        pass