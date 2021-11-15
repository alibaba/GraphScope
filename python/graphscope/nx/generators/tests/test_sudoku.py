"""Unit tests for the :mod:`graphscope.nx.generators.sudoku_graph` module."""

import networkx.generators.tests.test_sudoku
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx

import_as_graphscope_nx(
    networkx.generators.tests.test_sudoku,
    decorators=pytest.mark.usefixtures("graphscope_session"),
)
