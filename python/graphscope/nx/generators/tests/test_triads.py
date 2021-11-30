#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# This file is referred and derived from project NetworkX
#
# which has the following license:
#
# Copyright (C) 2004-2020, NetworkX Developers
# Aric Hagberg <hagberg@lanl.gov>
# Dan Schult <dschult@colgate.edu>
# Pieter Swart <swart@lanl.gov>
# All rights reserved.
#
# This file is part of NetworkX.
#
# NetworkX is distributed under a BSD license; see LICENSE.txt for more
# information.
#

"""Unit tests for the :mod:`graphscope.nx.generators.triads` module."""
import pytest

from graphscope.nx import triad_graph


def test_triad_graph():
    G = triad_graph("030T")
    assert [tuple(e) for e in ("ab", "ac", "cb")] == sorted(G.edges())


def test_invalid_name():
    with pytest.raises(ValueError):
        triad_graph("bogus")
