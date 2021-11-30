#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# This file is referred and derived from project NetworkX,
#
#  https://github.com/networkx/networkx/blob/master/networkx/readwrite/adjlist.py
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

from numbers import Number

# fmt: off
from networkx.drawing.nx_pylab import draw as _draw
from networkx.drawing.nx_pylab import draw_networkx as _draw_networkx
from networkx.drawing.nx_pylab import \
    draw_networkx_edge_labels as _draw_networkx_edge_labels
from networkx.drawing.nx_pylab import draw_networkx_edges as _draw_networkx_edges
from networkx.drawing.nx_pylab import draw_networkx_labels as _draw_networkx_labels
from networkx.drawing.nx_pylab import draw_networkx_nodes as _draw_networkx_nodes

import graphscope.nx as nx
from graphscope.nx.drawing.layout import circular_layout
from graphscope.nx.drawing.layout import kamada_kawai_layout
from graphscope.nx.drawing.layout import planar_layout
from graphscope.nx.drawing.layout import random_layout
from graphscope.nx.drawing.layout import shell_layout
from graphscope.nx.drawing.layout import spectral_layout
from graphscope.nx.drawing.layout import spring_layout
from graphscope.nx.utils.compat import with_graphscope_nx_context

# fmt: on


__all__ = [
    "draw",
    "draw_networkx",
    "draw_networkx_nodes",
    "draw_networkx_edges",
    "draw_networkx_labels",
    "draw_networkx_edge_labels",
    "draw_circular",
    "draw_kamada_kawai",
    "draw_random",
    "draw_spectral",
    "draw_spring",
    "draw_planar",
    "draw_shell",
]


def apply_alpha(colors, alpha, elem_list, cmap=None, vmin=None, vmax=None):
    """Apply an alpha (or list of alphas) to the colors provided.

    Parameters
    ----------

    colors : color string, or array of floats
       Color of element. Can be a single color format string (default='r'),
       or a  sequence of colors with the same length as nodelist.
       If numeric values are specified they will be mapped to
       colors using the cmap and vmin,vmax parameters.  See
       matplotlib.scatter for more details.

    alpha : float or array of floats
       Alpha values for elements. This can be a single alpha value, in
       which case it will be applied to all the elements of color. Otherwise,
       if it is an array, the elements of alpha will be applied to the colors
       in order (cycling through alpha multiple times if necessary).

    elem_list : array of networkx objects
       The list of elements which are being colored. These could be nodes,
       edges or labels.

    cmap : matplotlib colormap
       Color map for use if colors is a list of floats corresponding to points
       on a color mapping.

    vmin, vmax : float
       Minimum and maximum values for normalizing colors if a color mapping is
       used.

    Returns
    -------

    rgba_colors : numpy ndarray
        Array containing RGBA format values for each of the node colours.

    """
    from itertools import cycle
    from itertools import islice

    try:
        import matplotlib.cm as cm
        import numpy as np
        from matplotlib.colors import colorConverter
    except ImportError as e:
        raise ImportError("Matplotlib required for draw()") from e

    # If we have been provided with a list of numbers as long as elem_list,
    # apply the color mapping.
    if len(colors) == len(elem_list) and isinstance(colors[0], Number):
        mapper = cm.ScalarMappable(cmap=cmap)
        mapper.set_clim(vmin, vmax)
        rgba_colors = mapper.to_rgba(colors)
    # Otherwise, convert colors to matplotlib's RGB using the colorConverter
    # object.  These are converted to numpy ndarrays to be consistent with the
    # to_rgba method of ScalarMappable.
    else:
        try:
            rgba_colors = np.array([colorConverter.to_rgba(colors)])
        except ValueError:
            rgba_colors = np.array([colorConverter.to_rgba(color) for color in colors])
    # Set the final column of the rgba_colors to have the relevant alpha values
    try:
        # If alpha is longer than the number of colors, resize to the number of
        # elements.  Also, if rgba_colors.size (the number of elements of
        # rgba_colors) is the same as the number of elements, resize the array,
        # to avoid it being interpreted as a colormap by scatter()
        if len(alpha) > len(rgba_colors) or rgba_colors.size == len(elem_list):
            rgba_colors = np.resize(rgba_colors, (len(elem_list), 4))
            rgba_colors[1:, 0] = rgba_colors[0, 0]
            rgba_colors[1:, 1] = rgba_colors[0, 1]
            rgba_colors[1:, 2] = rgba_colors[0, 2]
        rgba_colors[:, 3] = list(islice(cycle(alpha), len(rgba_colors)))
    except TypeError:
        rgba_colors[:, -1] = alpha
    return rgba_colors


@with_graphscope_nx_context(_draw_networkx_nodes)
def draw_networkx_nodes(
    G,
    pos,
    nodelist=None,
    node_size=300,
    node_color="#1f78b4",
    node_shape="o",
    alpha=None,
    cmap=None,
    vmin=None,
    vmax=None,
    ax=None,
    linewidths=None,
    edgecolors=None,
    label=None,
):
    pass


@with_graphscope_nx_context(_draw_networkx_edges)
def draw_networkx_edges(
    G,
    pos,
    edgelist=None,
    width=1.0,
    edge_color="k",
    style="solid",
    alpha=None,
    arrowstyle="-|>",
    arrowsize=10,
    edge_cmap=None,
    edge_vmin=None,
    edge_vmax=None,
    ax=None,
    arrows=True,
    label=None,
    node_size=300,
    nodelist=None,
    node_shape="o",
    connectionstyle=None,
    min_source_margin=0,
    min_target_margin=0,
):
    pass


@with_graphscope_nx_context(_draw_networkx_labels)
def draw_networkx_labels(
    G,
    pos,
    labels=None,
    font_size=12,
    font_color="k",
    font_family="sans-serif",
    font_weight="normal",
    alpha=None,
    bbox=None,
    horizontalalignment="center",
    verticalalignment="center",
    ax=None,
):
    pass


@with_graphscope_nx_context(_draw)
def draw(G, pos=None, ax=None, **kwds):
    pass


@with_graphscope_nx_context(_draw_networkx)
def draw_networkx(G, pos=None, arrows=True, with_labels=True, **kwds):
    pass


@with_graphscope_nx_context(_draw_networkx_edge_labels)
def draw_networkx_edge_labels(
    G,
    pos,
    edge_labels=None,
    label_pos=0.5,
    font_size=10,
    font_color="k",
    font_family="sans-serif",
    font_weight="normal",
    alpha=None,
    bbox=None,
    horizontalalignment="center",
    verticalalignment="center",
    ax=None,
    rotate=True,
):
    pass


def draw_circular(G, **kwargs):
    draw(G, circular_layout(G), **kwargs)


def draw_kamada_kawai(G, **kwargs):
    draw(G, kamada_kawai_layout(G), **kwargs)


def draw_random(G, **kwargs):
    draw(G, random_layout(G), **kwargs)


def draw_spectral(G, **kwargs):
    draw(G, spectral_layout(G), **kwargs)


def draw_spring(G, **kwargs):
    draw(G, spring_layout(G), **kwargs)


def draw_shell(G, **kwargs):
    nlist = kwargs.get("nlist", None)
    if nlist is not None:
        del kwargs["nlist"]
    draw(G, shell_layout(G, nlist=nlist), **kwargs)


def draw_planar(G, **kwargs):
    draw(G, planar_layout(G), **kwargs)
