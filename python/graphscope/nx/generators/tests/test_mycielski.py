"""Unit tests for the :mod:`networkx.generators.mycielski` module."""

import networkx.generators.tests.test_mycielski
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx

import_as_graphscope_nx(
    networkx.generators.tests.test_mycielski,
    decorators=pytest.mark.usefixtures("graphscope_session"),
)
