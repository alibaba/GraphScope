#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# This file coreviews.py is referred and derived from project NetworkX,
#
#  https://github.com/networkx/networkx/blob/master/networkx/classes/coreviews.py
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

from networkx.classes.coreviews import AdjacencyView as _AdjacencyView
from networkx.classes.coreviews import AtlasView as _AtlasView

from graphscope.nx.utils.compat import patch_docstring

__all__ = [
    "AtlasView",
    "AdjacencyView",
]


@patch_docstring(_AtlasView)
class AtlasView(_AtlasView):
    def __eq__(self, other):
        return self._atlas.__eq__(other)

    def __contains__(self, key):
        return key in self._atlas


@patch_docstring(_AdjacencyView)
class AdjacencyView(AtlasView):
    __slots__ = ()  # Still uses AtlasView slots names _atlas

    def __getitem__(self, name):
        return AtlasView(self._atlas[name])
