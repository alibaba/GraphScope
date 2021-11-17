"""Unit tests for the :mod:`networkx.generators.cographs` module.

"""

import networkx.generators.tests.test_cographs
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx

import_as_graphscope_nx(
    networkx.generators.tests.test_cographs,
    decorators=pytest.mark.usefixtures("graphscope_session"),
)
