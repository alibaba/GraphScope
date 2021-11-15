"""Unit tests for the :mod:`graphscope.nx.generators.triads` module."""
import pytest

from graphscope.nx import triad_graph


def test_triad_graph():
    G = triad_graph("030T")
    assert [tuple(e) for e in ("ab", "ac", "cb")] == sorted(G.edges())


def test_invalid_name():
    with pytest.raises(ValueError):
        triad_graph("bogus")
