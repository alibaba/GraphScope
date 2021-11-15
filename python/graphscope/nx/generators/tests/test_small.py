"""Generators - Small
=====================

Some small graphs
"""


import pytest
from networkx.generators.tests.test_small import TestGeneratorsSmall

from graphscope.framework.errors import UnimplementedError
from graphscope.nx.utils.compat import with_graphscope_nx_context


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestGeneratorsSmall)
class TestGeneratorsSmall:
    def test_properties_named_small_graphs(self):
        pass
