# -*- coding: utf-8 -*-
#
# This file random_graphs.py is referred and derived from project NetworkX,
#
#  https://github.com/networkx/networkx/blob/master/networkx/generators/random_graphs.py
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
Generators for random graphs.

"""

import itertools
import math
from collections import defaultdict

import networkx as nxa
from networkx.utils import powerlaw_sequence
from networkx.utils import py_random_state

from graphscope import nx
from graphscope.nx.generators.classic import complete_graph
from graphscope.nx.generators.classic import empty_graph
from graphscope.nx.generators.classic import path_graph
from graphscope.nx.generators.degree_seq import degree_sequence_tree
from graphscope.nx.utils.compat import patch_docstring

__all__ = [
    "fast_gnp_random_graph",
    "gnp_random_graph",
    "dense_gnm_random_graph",
    "gnm_random_graph",
    "erdos_renyi_graph",
    "binomial_graph",
    "newman_watts_strogatz_graph",
    "watts_strogatz_graph",
    "connected_watts_strogatz_graph",
    "random_regular_graph",
    "barabasi_albert_graph",
    "dual_barabasi_albert_graph",
    "extended_barabasi_albert_graph",
    "powerlaw_cluster_graph",
    "random_lobster",
    "random_shell_graph",
    "random_powerlaw_tree",
    "random_powerlaw_tree_sequence",
    "random_kernel_graph",
]

# -------------------------------------------------------------------------
#  Some Famous Random Graphs
# -------------------------------------------------------------------------


@patch_docstring(nxa.fast_gnp_random_graph)
@py_random_state(2)
def fast_gnp_random_graph(n, p, seed=None, directed=False):
    G = empty_graph(n)

    if p <= 0 or p >= 1:
        return nx.gnp_random_graph(n, p, seed=seed, directed=directed)

    w = -1
    lp = math.log(1.0 - p)

    if directed:
        G = nx.DiGraph(G)
        # Nodes in graph are from 0,n-1 (start with v as the first node index).
        v = 0
        while v < n:
            lr = math.log(1.0 - seed.random())
            w = w + 1 + int(lr / lp)
            if v == w:  # avoid self loops
                w = w + 1
            while v < n <= w:
                w = w - n
                v = v + 1
                if v == w:  # avoid self loops
                    w = w + 1
            if v < n:
                G.add_edge(v, w)
    else:
        # Nodes in graph are from 0,n-1 (start with v as the second node index).
        v = 1
        while v < n:
            lr = math.log(1.0 - seed.random())
            w = w + 1 + int(lr / lp)
            while w >= v and v < n:
                w = w - v
                v = v + 1
            if v < n:
                G.add_edge(v, w)
    return G


@patch_docstring(nxa.gnp_random_graph)
@py_random_state(2)
def gnp_random_graph(n, p, seed=None, directed=False):
    if directed:
        edges = itertools.permutations(range(n), 2)
        G = nx.DiGraph()
    else:
        edges = itertools.combinations(range(n), 2)
        G = nx.Graph()
    G.add_nodes_from(range(n))
    if p <= 0:
        return G
    if p >= 1:
        return complete_graph(n, create_using=G)

    for e in edges:
        if seed.random() < p:
            G.add_edge(*e)
    return G


# add some aliases to common names
binomial_graph = gnp_random_graph
erdos_renyi_graph = gnp_random_graph


@patch_docstring(nxa.dense_gnm_random_graph)
@py_random_state(2)
def dense_gnm_random_graph(n, m, seed=None):
    mmax = n * (n - 1) / 2
    if m >= mmax:
        G = complete_graph(n)
    else:
        G = empty_graph(n)

    if n == 1 or m >= mmax:
        return G

    u = 0
    v = 1
    t = 0
    k = 0
    while True:
        if seed.randrange(mmax - t) < m - k:
            G.add_edge(u, v)
            k += 1
            if k == m:
                return G
        t += 1
        v += 1
        if v == n:  # go to next row of adjacency matrix
            u += 1
            v = u + 1


@patch_docstring(nxa.gnm_random_graph)
@py_random_state(2)
def gnm_random_graph(n, m, seed=None, directed=False):
    if directed:
        G = nx.DiGraph()
    else:
        G = nx.Graph()
    G.add_nodes_from(range(n))

    if n == 1:
        return G
    max_edges = n * (n - 1)
    if not directed:
        max_edges /= 2.0
    if m >= max_edges:
        return complete_graph(n, create_using=G)

    nlist = list(G)
    edge_count = 0
    while edge_count < m:
        # generate random edge,u,v
        u = seed.choice(nlist)
        v = seed.choice(nlist)
        if u == v:
            continue
        else:
            G.add_edge(u, v)
            edge_count = edge_count + 1
    return G


@patch_docstring(nxa.newman_watts_strogatz_graph)
@py_random_state(3)
def newman_watts_strogatz_graph(n, k, p, seed=None):
    if k > n:
        raise nx.NetworkXError("k>=n, choose smaller k or larger n")

    # If k == n the graph return is a complete graph
    if k == n:
        return nx.complete_graph(n)

    G = empty_graph(n)
    nlist = list(G.nodes())
    fromv = nlist
    # connect the k/2 neighbors
    for j in range(1, k // 2 + 1):
        tov = fromv[j:] + fromv[0:j]  # the first j are now last
        for i, value in enumerate(fromv):
            G.add_edge(value, tov[i])
    # for each edge u-v, with probability p, randomly select existing
    # node w and add new edge u-w
    e = list(G.edges())
    for u, v in e:
        if seed.random() < p:
            w = seed.choice(nlist)
            # no self-loops and reject if edge u-w exists
            # is that the correct NWS model?
            while w == u or G.has_edge(u, w):
                w = seed.choice(nlist)
                if G.degree(u) >= n - 1:
                    break  # skip this rewiring
            else:
                G.add_edge(u, w)
    return G


@patch_docstring(nxa.watts_strogatz_graph)
@py_random_state(3)
def watts_strogatz_graph(n, k, p, seed=None):
    if k > n:
        raise nx.NetworkXError("k>n, choose smaller k or larger n")

    # If k == n, the graph is complete not Watts-Strogatz
    if k == n:
        return nx.complete_graph(n)

    G = nx.Graph()
    nodes = list(range(n))  # nodes are labeled 0 to n-1
    # connect each node to k/2 neighbors
    for j in range(1, k // 2 + 1):
        targets = nodes[j:] + nodes[0:j]  # first j nodes are now last in list
        G.add_edges_from(zip(nodes, targets))
    # rewire edges from each node
    # loop over all nodes in order (label) and neighbors in order (distance)
    # no self loops or multiple edges allowed
    for j in range(1, k // 2 + 1):  # outer loop is neighbors
        targets = nodes[j:] + nodes[0:j]  # first j nodes are now last in list
        # inner loop in node order
        for u, v in zip(nodes, targets):
            if seed.random() < p:
                w = seed.choice(nodes)
                # Enforce no self-loops or multiple edges
                while w == u or G.has_edge(u, w):
                    w = seed.choice(nodes)
                    if G.degree(u) >= n - 1:
                        break  # skip this rewiring
                else:
                    G.remove_edge(u, v)
                    G.add_edge(u, w)
    return G


@patch_docstring(nxa.connected_watts_strogatz_graph)
@py_random_state(4)
def connected_watts_strogatz_graph(n, k, p, tries=100, seed=None):
    for i in range(tries):
        # seed is an RNG so should change sequence each call
        G = watts_strogatz_graph(n, k, p, seed)
        if nx.is_connected(G):
            return G
    raise nx.NetworkXError("Maximum number of tries exceeded")


@patch_docstring(nxa.random_regular_graph)
@py_random_state(2)
def random_regular_graph(d, n, seed=None):
    if (n * d) % 2 != 0:
        raise nx.NetworkXError("n * d must be even")

    if not 0 <= d < n:
        raise nx.NetworkXError("the 0 <= d < n inequality must be satisfied")

    if d == 0:
        return empty_graph(n)

    def _suitable(edges, potential_edges):
        # Helper subroutine to check if there are suitable edges remaining
        # If False, the generation of the graph has failed
        if not potential_edges:
            return True
        for s1 in potential_edges:
            for s2 in potential_edges:
                # Two iterators on the same dictionary are guaranteed
                # to visit it in the same order if there are no
                # intervening modifications.
                if s1 == s2:
                    # Only need to consider s1-s2 pair one time
                    break
                if s1 > s2:
                    s1, s2 = s2, s1
                if (s1, s2) not in edges:
                    return True
        return False

    def _try_creation():
        # Attempt to create an edge set

        edges = set()
        stubs = list(range(n)) * d

        while stubs:
            potential_edges = defaultdict(lambda: 0)
            seed.shuffle(stubs)
            stubiter = iter(stubs)
            for s1, s2 in zip(stubiter, stubiter):
                if s1 > s2:
                    s1, s2 = s2, s1
                if s1 != s2 and ((s1, s2) not in edges):
                    edges.add((s1, s2))
                else:
                    potential_edges[s1] += 1
                    potential_edges[s2] += 1

            if not _suitable(edges, potential_edges):
                return None  # failed to find suitable edge set

            stubs = [
                node
                for node, potential in potential_edges.items()
                for _ in range(potential)
            ]
        return edges

    # Even though a suitable edge set exists,
    # the generation of such a set is not guaranteed.
    # Try repeatedly to find one.
    edges = _try_creation()
    while edges is None:
        edges = _try_creation()

    G = nx.Graph()
    G.add_edges_from(edges)

    return G


def _random_subset(seq, m, rng):
    """Return m unique elements from seq.

    This differs from random.sample which can return repeated
    elements if seq holds repeated elements.

    Note: rng is a random.Random or numpy.random.RandomState instance.
    """
    targets = set()
    while len(targets) < m:
        x = rng.choice(seq)
        targets.add(x)
    return targets


@patch_docstring(nxa.barabasi_albert_graph)
@py_random_state(2)
def barabasi_albert_graph(n, m, seed=None, initial_graph=None):
    if m < 1 or m >= n:
        raise nx.NetworkXError(
            "Barabási–Albert network must have m >= 1"
            " and m < n, m = %d, n = %d" % (m, n)
        )

    if initial_graph is None:
        # Default initial graph : star graph on (m + 1) nodes
        G = nx.star_graph(m)
    else:
        if len(initial_graph) < m or len(initial_graph) > n:
            raise nx.NetworkXError(
                f"Barabási–Albert initial graph needs between m={m} and n={n} nodes"
            )
        G = initial_graph.copy()

    # List of existing nodes, with nodes repeated once for each adjacent edge
    repeated_nodes = [n for n, d in G.degree() for _ in range(d)]
    # Start adding the other n - m0 nodes.
    source = len(G)
    while source < n:
        # Now choose m unique nodes from the existing nodes
        # Pick uniformly from repeated_nodes (preferential attachment)
        targets = _random_subset(repeated_nodes, m, seed)
        # Add edges to m nodes from the source.
        G.add_edges_from(zip([source] * m, targets))
        # Add one node to the list for each new edge just created.
        repeated_nodes.extend(targets)
        # And the new node "source" has m edges to add to the list.
        repeated_nodes.extend([source] * m)

        source += 1
    return G


@patch_docstring(nxa.dual_barabasi_albert_graph)
@py_random_state(4)
def dual_barabasi_albert_graph(n, m1, m2, p, seed=None, initial_graph=None):
    if m1 < 1 or m1 >= n:
        raise nx.NetworkXError(
            "Dual Barabási–Albert network must have m1 >= 1"
            " and m1 < n, m1 = %d, n = %d" % (m1, n)
        )
    if m2 < 1 or m2 >= n:
        raise nx.NetworkXError(
            "Dual Barabási–Albert network must have m2 >= 1"
            " and m2 < n, m2 = %d, n = %d" % (m2, n)
        )
    if p < 0 or p > 1:
        raise nx.NetworkXError(
            "Dual Barabási–Albert network must have 0 <= p <= 1," "p = %f" % p
        )

    # For simplicity, if p == 0 or 1, just return BA
    if p == 1:
        return barabasi_albert_graph(n, m1, seed)
    if p == 0:
        return barabasi_albert_graph(n, m2, seed)

    if initial_graph is None:
        # Default initial graph : empty graph on max(m1, m2) nodes
        G = nx.star_graph(max(m1, m2))
    else:
        if len(initial_graph) < max(m1, m2) or len(initial_graph) > n:
            raise nx.NetworkXError(
                f"Barabási–Albert initial graph must have between "
                f"max(m1, m2) = {max(m1, m2)} and n = {n} nodes"
            )
        G = initial_graph.copy()

    # Target nodes for new edges
    targets = list(G)
    # List of existing nodes, with nodes repeated once for each adjacent edge
    repeated_nodes = [n for n, d in G.degree() for _ in range(d)]
    # Start adding the remaining nodes.
    source = len(G)
    while source < n:
        # Pick which m to use (m1 or m2)
        if seed.random() < p:
            m = m1
        else:
            m = m2
        # Now choose m unique nodes from the existing nodes
        # Pick uniformly from repeated_nodes (preferential attachment)
        targets = _random_subset(repeated_nodes, m, seed)
        # Add edges to m nodes from the source.
        G.add_edges_from(zip([source] * m, targets))
        # Add one node to the list for each new edge just created.
        repeated_nodes.extend(targets)
        # And the new node "source" has m edges to add to the list.
        repeated_nodes.extend([source] * m)

        source += 1
    return G


@patch_docstring(nxa.extended_barabasi_albert_graph)
@py_random_state(4)
def extended_barabasi_albert_graph(n, m, p, q, seed=None):
    if m < 1 or m >= n:
        msg = "Extended Barabasi-Albert network needs m>=1 and m<n, m=%d, n=%d"
        raise nx.NetworkXError(msg % (m, n))
    if p + q >= 1:
        msg = "Extended Barabasi-Albert network needs p + q <= 1, p=%d, q=%d"
        raise nx.NetworkXError(msg % (p, q))

    # Add m initial nodes (m0 in barabasi-speak)
    G = empty_graph(m)

    # List of nodes to represent the preferential attachment random selection.
    # At the creation of the graph, all nodes are added to the list
    # so that even nodes that are not connected have a chance to get selected,
    # for rewiring and adding of edges.
    # With each new edge, nodes at the ends of the edge are added to the list.
    attachment_preference = []
    attachment_preference.extend(range(m))

    # Start adding the other n-m nodes. The first node is m.
    new_node = m
    while new_node < n:
        a_probability = seed.random()

        # Total number of edges of a Clique of all the nodes
        clique_degree = len(G) - 1
        clique_size = (len(G) * clique_degree) / 2

        # Adding m new edges, if there is room to add them
        if a_probability < p and G.size() <= clique_size - m:
            # Select the nodes where an edge can be added
            elligible_nodes = [nd for nd, deg in G.degree() if deg < clique_degree]
            for i in range(m):
                # Choosing a random source node from elligible_nodes
                src_node = seed.choice(elligible_nodes)

                # Picking a possible node that is not 'src_node' or
                # neighbor with 'src_node', with preferential attachment
                prohibited_nodes = list(G[src_node])
                prohibited_nodes.append(src_node)
                # This will raise an exception if the sequence is empty
                dest_node = seed.choice(
                    [nd for nd in attachment_preference if nd not in prohibited_nodes]
                )
                # Adding the new edge
                G.add_edge(src_node, dest_node)

                # Appending both nodes to add to their preferential attachment
                attachment_preference.append(src_node)
                attachment_preference.append(dest_node)

                # Adjusting the elligible nodes. Degree may be saturated.
                if G.degree(src_node) == clique_degree:
                    elligible_nodes.remove(src_node)
                if (
                    G.degree(dest_node) == clique_degree
                    and dest_node in elligible_nodes
                ):
                    elligible_nodes.remove(dest_node)

        # Rewiring m edges, if there are enough edges
        elif p <= a_probability < (p + q) and m <= G.size() < clique_size:
            # Selecting nodes that have at least 1 edge but that are not
            # fully connected to ALL other nodes (center of star).
            # These nodes are the pivot nodes of the edges to rewire
            elligible_nodes = [nd for nd, deg in G.degree() if 0 < deg < clique_degree]
            for i in range(m):
                # Choosing a random source node
                node = seed.choice(elligible_nodes)

                # The available nodes do have a neighbor at least.
                neighbor_nodes = list(G[node])

                # Choosing the other end that will get detached
                src_node = seed.choice(neighbor_nodes)

                # Picking a target node that is not 'node' or
                # neighbor with 'node', with preferential attachment
                neighbor_nodes.append(node)
                dest_node = seed.choice(
                    [nd for nd in attachment_preference if nd not in neighbor_nodes]
                )
                # Rewire
                G.remove_edge(node, src_node)
                G.add_edge(node, dest_node)

                # Adjusting the preferential attachment list
                attachment_preference.remove(src_node)
                attachment_preference.append(dest_node)

                # Adjusting the elligible nodes.
                # nodes may be saturated or isolated.
                if G.degree(src_node) == 0 and src_node in elligible_nodes:
                    elligible_nodes.remove(src_node)
                if dest_node in elligible_nodes:
                    if G.degree(dest_node) == clique_degree:
                        elligible_nodes.remove(dest_node)
                else:
                    if G.degree(dest_node) == 1:
                        elligible_nodes.append(dest_node)

        # Adding new node with m edges
        else:
            # Select the edges' nodes by preferential attachment
            targets = _random_subset(attachment_preference, m, seed)
            G.add_edges_from(zip([new_node] * m, targets))

            # Add one node to the list for each new edge just created.
            attachment_preference.extend(targets)
            # The new node has m edges to it, plus itself: m + 1
            attachment_preference.extend([new_node] * (m + 1))
            new_node += 1
    return G


@patch_docstring(nxa.powerlaw_cluster_graph)
@py_random_state(3)
def powerlaw_cluster_graph(n, m, p, seed=None):
    if m < 1 or n < m:
        raise nx.NetworkXError(
            "NetworkXError must have m>1 and m<n, m=%d,n=%d" % (m, n)
        )

    if p > 1 or p < 0:
        raise nx.NetworkXError("NetworkXError p must be in [0,1], p=%f" % (p))

    G = empty_graph(m)  # add m initial nodes (m0 in barabasi-speak)
    repeated_nodes = list(G.nodes())  # list of existing nodes to sample from
    # with nodes repeated once for each adjacent edge
    source = m  # next node is m
    while source < n:  # Now add the other n-1 nodes
        possible_targets = _random_subset(repeated_nodes, m, seed)
        # do one preferential attachment for new node
        target = possible_targets.pop()
        G.add_edge(source, target)
        repeated_nodes.append(target)  # add one node to list for each new link
        count = 1
        while count < m:  # add m-1 more new links
            if seed.random() < p:  # clustering step: add triangle
                neighborhood = [
                    nbr
                    for nbr in G.neighbors(target)
                    if not G.has_edge(source, nbr) and not nbr == source
                ]
                if neighborhood:  # if there is a neighbor without a link
                    nbr = seed.choice(neighborhood)
                    G.add_edge(source, nbr)  # add triangle
                    repeated_nodes.append(nbr)
                    count = count + 1
                    continue  # go to top of while loop
            # else do preferential attachment step if above fails
            target = possible_targets.pop()
            G.add_edge(source, target)
            repeated_nodes.append(target)
            count = count + 1

        repeated_nodes.extend([source] * m)  # add source node to list m times
        source += 1
    return G


@patch_docstring(nxa.random_lobster)
@py_random_state(3)
def random_lobster(n, p1, p2, seed=None):
    # a necessary ingredient in any self-respecting graph library
    llen = int(2 * seed.random() * n + 0.5)
    L = path_graph(llen)
    # build caterpillar: add edges to path graph with probability p1
    current_node = llen - 1
    for n in range(llen):
        if seed.random() < p1:  # add fuzzy caterpillar parts
            current_node += 1
            L.add_edge(n, current_node)
            if seed.random() < p2:  # add crunchy lobster bits
                current_node += 1
                L.add_edge(current_node - 1, current_node)
    return L  # voila, un lobster!


@patch_docstring(nxa.random_shell_graph)
@py_random_state(1)
def random_shell_graph(constructor, seed=None):
    G = empty_graph(0)

    glist = []
    intra_edges = []
    nnodes = 0
    # create gnm graphs for each shell
    for n, m, d in constructor:
        inter_edges = int(m * d)
        intra_edges.append(m - inter_edges)
        g = nx.convert_node_labels_to_integers(
            gnm_random_graph(n, inter_edges, seed=seed), first_label=nnodes
        )
        glist.append(g)
        nnodes += n
        G = nx.operators.union(G, g)

    # connect the shells randomly
    for gi in range(len(glist) - 1):
        nlist1 = list(glist[gi])
        nlist2 = list(glist[gi + 1])
        total_edges = intra_edges[gi]
        edge_count = 0
        while edge_count < total_edges:
            u = seed.choice(nlist1)
            v = seed.choice(nlist2)
            if u == v or G.has_edge(u, v):
                continue
            else:
                G.add_edge(u, v)
                edge_count = edge_count + 1
    return G


@patch_docstring(nxa.random_powerlaw_tree)
@py_random_state(2)
def random_powerlaw_tree(n, gamma=3, seed=None, tries=100):
    # This call may raise a NetworkXError if the number of tries is succeeded.
    seq = random_powerlaw_tree_sequence(n, gamma=gamma, seed=seed, tries=tries)
    G = degree_sequence_tree(seq)
    return G


@patch_docstring(nxa.random_powerlaw_tree_sequence)
@py_random_state(2)
def random_powerlaw_tree_sequence(n, gamma=3, seed=None, tries=100):
    # get trial sequence
    z = powerlaw_sequence(n, exponent=gamma, seed=seed)
    # round to integer values in the range [0,n]
    zseq = [min(n, max(int(round(s)), 0)) for s in z]

    # another sequence to swap values from
    z = powerlaw_sequence(tries, exponent=gamma, seed=seed)
    # round to integer values in the range [0,n]
    swap = [min(n, max(int(round(s)), 0)) for s in z]

    for deg in swap:
        # If this degree sequence can be the degree sequence of a tree, return
        # it. It can be a tree if the number of edges is one fewer than the
        # number of nodes, or in other words, `n - sum(zseq) / 2 == 1`. We
        # use an equivalent condition below that avoids floating point
        # operations.
        if 2 * n - sum(zseq) == 2:
            return zseq
        index = seed.randint(0, n - 1)
        zseq[index] = swap.pop()

    raise nx.NetworkXError(
        "Exceeded max (%d) attempts for a valid tree" " sequence." % tries
    )


@patch_docstring(nxa.random_kernel_graph)
@py_random_state(3)
def random_kernel_graph(n, kernel_integral, kernel_root=None, seed=None):
    if kernel_root is None:
        import scipy.optimize as optimize

        def kernel_root(y, a, r):
            def my_function(b):
                return kernel_integral(y, a, b) - r

            return optimize.brentq(my_function, a, 1)

    graph = nx.Graph()
    graph.add_nodes_from(range(n))
    (i, j) = (1, 1)
    while i < n:
        r = -math.log(1 - seed.random())  # (1-seed.random()) in (0, 1]
        if kernel_integral(i / n, j / n, 1) <= r:
            i, j = i + 1, i + 1
        else:
            j = int(math.ceil(n * kernel_root(i / n, j / n, r)))
            graph.add_edge(i - 1, j - 1)
    return graph
