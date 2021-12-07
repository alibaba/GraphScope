#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# This file convert_matrix.py is referred and derived from project NetworkX,
#
#  https://github.com/networkx/networkx/blob/master/networkx/convert_matrix.py
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

import networkx.convert_matrix

from graphscope.nx.utils.compat import import_as_graphscope_nx

import_as_graphscope_nx(networkx.convert_matrix)
