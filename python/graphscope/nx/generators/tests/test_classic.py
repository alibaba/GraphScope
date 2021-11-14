"""
====================
Generators - Classic
====================

Unit tests for various classic graph generators in generators/classic.py
"""
import pytest
from networkx.generators.tests.test_classic import TestGeneratorClassic

from graphscope.nx.utils.compat import with_graphscope_nx_context


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestGeneratorClassic)
class TestGeneratorClassic:
    @pytest.mark.skip(reason="FIXME: test take too much time.")
    def test_dorogovtsev_goltsev_mendes_graph(self):
        pass
