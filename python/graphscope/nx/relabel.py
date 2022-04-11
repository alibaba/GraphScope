#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# This file relabel.py is referred and derived from project NetworkX,
#
#  https://github.com/networkx/networkx/blob/master/networkx/relabel.py
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

"""
GraphScope's compatible layer for :mod:`networkx.relabel`.
"""

from networkx.relabel import convert_node_labels_to_integers as _cnlti
from networkx.relabel import relabel_nodes as _relabel_nodes

from graphscope import nx

# from graphscope.nx.utils.compat import import_as_graphscope_nx
from graphscope.nx.utils.compat import patch_docstring

__all__ = ["convert_node_labels_to_integers", "relabel_nodes"]


@patch_docstring(_relabel_nodes)
def relabel_nodes(G, mapping, copy=True):
    # you can pass a function f(old_label)->new_label
    # but we'll just make a dictionary here regardless
    if not hasattr(mapping, "__getitem__"):
        m = {n: mapping(n) for n in G}
    else:
        m = mapping
    if copy:
        return _relabel_copy(G, m)
    return _relabel_inplace(G, m)


def _relabel_inplace(G, mapping):
    old_labels = set(mapping.keys())
    new_labels = set(mapping.values())
    if len(old_labels & new_labels) > 0:
        # labels sets overlap
        # can we topological sort and still do the relabeling?
        D = nx.DiGraph(list(mapping.items()))
        D.remove_edges_from(nx.selfloop_edges(D))
        try:
            nodes = reversed(list(nx.topological_sort(D)))
        except nx.NetworkXUnfeasible as e:
            raise nx.NetworkXUnfeasible(
                "The node label sets are overlapping and no ordering can "
                "resolve the mapping. Use copy=True."
            ) from e
    else:
        # non-overlapping label sets
        nodes = old_labels

    multigraph = G.is_multigraph()
    directed = G.is_directed()

    for old in nodes:
        # Test that old is in both mapping and G, otherwise ignore.
        try:
            new = mapping[old]
            G.add_node(new, **G.nodes[old])
        except KeyError:
            continue
        if new == old:
            continue
        if multigraph:
            new_edges = [
                (new, new if old == target else target, key, data)
                for (_, target, key, data) in G.edges(old, data=True, keys=True)
            ]
            if directed:
                new_edges += [
                    (new if old == source else source, new, key, data)
                    for (source, _, key, data) in G.in_edges(old, data=True, keys=True)
                ]
            # Ensure new edges won't overwrite existing ones
            seen = set()
            for i, (source, target, key, data) in enumerate(new_edges):
                if target in G[source] and key in G[source][target]:
                    new_key = 0 if not isinstance(key, (int, float)) else key
                    while new_key in G[source][target] or (target, new_key) in seen:
                        new_key += 1
                    new_edges[i] = (source, target, new_key, data)
                    seen.add((target, new_key))
        else:
            new_edges = [
                (new, new if old == target else target, data)
                for (_, target, data) in G.edges(old, data=True)
            ]
            if directed:
                new_edges += [
                    (new if old == source else source, new, data)
                    for (source, _, data) in G.in_edges(old, data=True)
                ]
        G.remove_node(old)
        G.add_edges_from(new_edges)
    return G


def _relabel_copy(G, mapping):
    H = G.__class__()
    H.add_nodes_from([(mapping.get(n, n), d.copy()) for n, d in G.nodes.items()])
    if G.is_multigraph():
        new_edges = [
            (mapping.get(n1, n1), mapping.get(n2, n2), k, d.copy())
            for (n1, n2, k, d) in G.edges(keys=True, data=True)
        ]

        # check for conflicting edge-keys
        undirected = not G.is_directed()
        seen_edges = set()
        for i, (source, target, key, data) in enumerate(new_edges):
            while (source, target, key) in seen_edges:
                if not isinstance(key, (int, float)):
                    key = 0
                key += 1
            seen_edges.add((source, target, key))
            if undirected:
                seen_edges.add((target, source, key))
            new_edges[i] = (source, target, key, data)

        H.add_edges_from(new_edges)
    else:
        H.add_edges_from(
            (mapping.get(n1, n1), mapping.get(n2, n2), d.copy())
            for (n1, n2, d) in G.edges(data=True)
        )
    H.graph.update(G.graph)
    return H


@patch_docstring(_cnlti)
def convert_node_labels_to_integers(
    G, first_label=0, ordering="default", label_attribute=None
):
    N = G.number_of_nodes() + first_label
    if ordering == "default":
        mapping = dict(zip(G.nodes(), range(first_label, N)))
    elif ordering == "sorted":
        nlist = sorted(G.nodes())
        mapping = dict(zip(nlist, range(first_label, N)))
    elif ordering == "increasing degree":
        dv_pairs = [(d, n) for (n, d) in G.degree()]
        dv_pairs.sort()  # in-place sort from lowest to highest degree
        mapping = dict(zip([n for d, n in dv_pairs], range(first_label, N)))
    elif ordering == "decreasing degree":
        dv_pairs = [(d, n) for (n, d) in G.degree()]
        dv_pairs.sort()  # in-place sort from lowest to highest degree
        dv_pairs.reverse()
        mapping = dict(zip([n for d, n in dv_pairs], range(first_label, N)))
    else:
        raise nx.NetworkXError(f"Unknown node ordering: {ordering}")
    H = relabel_nodes(G, mapping)
    # create node attribute with the old label
    if label_attribute is not None:
        nx.set_node_attributes(H, {v: k for k, v in mapping.items()}, label_attribute)
    return H
