"""
====================
Generators - Non Isomorphic Trees
====================

Unit tests for WROM algorithm generator in generators/nonisomorphic_trees.py
"""
import networkx.generators.tests.test_nonisomorphic_trees
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx

import_as_graphscope_nx(
    networkx.generators.tests.test_nonisomorphic_trees,
    decorators=pytest.mark.usefixtures("graphscope_session"),
)
