#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright 2020 Alibaba Group Holding Limited. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import json
import sys

import ipywidgets as widgets
from spectate import mvc
from traitlets import TraitType
from traitlets import Unicode

from ._frontend import module_name
from ._frontend import module_version


class Mutable(TraitType):
    """A base class for mutable traits using Spectate"""

    _model_type = None
    _event_type = "change"

    def instance_init(self, obj):
        default = self._model_type()

        @mvc.view(default)
        def callback(default, events):
            change = dict(
                new=getattr(obj, self.name),
                name=self.name,
                type=self._event_type,
            )
            obj.notify_change(change)

        setattr(obj, self.name, default)


class MutableDict(Mutable):
    """A mutable dictionary trait"""

    _model_type = mvc.Dict


@widgets.register
class GraphModel(widgets.DOMWidget):
    """Graph Widget"""

    # Name of the widget model class in front-end
    _model_name = Unicode("GraphModel").tag(sync=True)

    # Name of the front-end module containing widget model
    _model_module = Unicode(module_name).tag(sync=True)

    # Version of the front-end module containing widget model
    _model_module_version = Unicode(module_version).tag(sync=True)

    # Name of the widget view class in front-end
    _view_name = Unicode("GraphView").tag(sync=True)

    # Name of the front-end module containing widget view
    _view_module = Unicode(module_name).tag(sync=True)

    # Version of the front-end module containing widget view
    _view_module_version = Unicode(module_version).tag(sync=True)

    # Widget specific property.
    # Widget properties are defined as traitlets. Any property tagged with `sync=True`
    # is automatically synced to the frontend *any* time it changes in Python.
    # It is synced back to Python from the frontend *any* time the model is touched.
    # data: { nodes: [], edges: [] }

    value_dict = {}
    value = Unicode("").tag(sync=True)

    _interactive_query = None

    _nodes_id_map = {}
    _nodes_id_dict = {}
    _edges_id_dict = {}

    _default_data = {
        "nodes": [
            {
                "id": "1",
                "label": "person",
                "nodeType": "person",
            },
            {
                "id": "2",
                "label": "person",
                "nodeType": "person",
            },
            {
                "id": "3",
                "label": "person",
                "nodeType": "person",
            },
        ],
        "edges": [
            {
                "label": "knows",
                "source": "1",
                "target": "2",
            },
            {
                "label": "knows",
                "source": "2",
                "target": "3",
            },
            {
                "label": "knows",
                "source": "3",
                "target": "1",
            },
        ],
    }

    def addGraphFromData(self, data=None):
        if data is None:
            data = self._default_data
        nodeList = []
        edgeList = []

        def _addNodes(nodes):
            for node in nodes:
                current = {}
                current["id"] = str(node["id"])
                current["label"] = node["label"]
                current["parentId"] = ""
                current["level"] = 0
                current["degree"] = 1  # need to update
                current["count"] = 0
                current["nodeType"] = node["nodeType"]
                current["properties"] = {}
                nodeList.append(current)

        def _addEdges(list_edge):
            for e in list_edge:
                edge = {}
                edge["label"] = e["label"]
                edge["source"] = str(e["source"])
                edge["target"] = str(e["target"])
                edge["count"] = 0
                # edge["edgeType"] = e['edgeType']
                edge["properties"] = {}
                edgeList.append(edge)

        _addNodes(data["nodes"])
        _addEdges(data["edges"])

        data_dict = {}
        data_dict["graphVisId"] = "0"
        data_dict["nodes"] = nodeList
        data_dict["edges"] = edgeList

        data_str = json.dumps(data_dict)
        self.value = data_str

    def _gremlin(self, query=""):
        return self._interactive_query.execute(query).all().result()

    def _process_vertices_1_hop(self, vertices):
        nodes = []
        edges = []

        def _process_node(list_id, list_val, list_prop):
            for i, item in enumerate(list_id):
                vid = str(item.id)
                if vid in self._nodes_id_dict:
                    continue
                #
                node = {}
                node["id"] = vid
                node["oid"] = str(list_val[i])
                node["parentId"] = ""
                node["label"] = str(item.label)
                node["level"] = 0
                node["degree"] = 1  # need to update
                node["count"] = 0
                node["nodeType"] = str(item.label)
                node["properties"] = list_prop[i]
                self._nodes_id_dict[vid] = True
                self._nodes_id_map[vid] = str(list_val[i])
                nodes.append(node)

        def _process_edge(list_edge):
            for e in list_edge:
                edge = {}
                edge["id"] = str(e.id)
                #
                if edge["id"] in self._edges_id_dict:
                    continue
                #
                edge["label"] = e.label
                edge["source"] = str(e.outV.id)
                edge["target"] = str(e.inV.id)
                # edge["source"] = self._nodes_id_map[str(e.outV.id)]
                # edge["target"] = self._nodes_id_map[str(e.inV.id)]
                edge["count"] = 0
                edge["edgeType"] = e.label
                edge["properties"] = {}
                self._edges_id_dict[edge["id"]] = True
                edges.append(edge)

        for vert in vertices:
            vert_str = str(vert)
            # node
            list_id = self._gremlin("g.V().has('id'," + vert_str + ")")
            list_id_val = self._gremlin("g.V().has('id'," + vert_str + ").values('id')")
            list_id_prop = self._gremlin("g.V().has('id'," + vert_str + ").valueMap()")
            _process_node(list_id, list_id_val, list_id_prop)
            #
            list_id_inV = self._gremlin(
                "g.V().has('id'," + vert_str + ").outE().inV().order().by('id',incr)"
            )
            list_id_inV_val = self._gremlin(
                "g.V().has('id',"
                + vert_str
                + ").outE().inV().order().by('id',incr).values('id')"
            )
            list_id_inV_prop = self._gremlin(
                "g.V().has('id',"
                + vert_str
                + ").outE().inV().order().by('id',incr).valueMap()"
            )
            _process_node(list_id_inV, list_id_inV_val, list_id_inV_prop)
            #
            list_id_outV = self._gremlin(
                "g.V().has('id'," + vert_str + ").inE().outV().order().by('id',incr)"
            )
            list_id_outV_val = self._gremlin(
                "g.V().has('id',"
                + vert_str
                + ").inE().outV().order().by('id',incr).values('id')"
            )
            list_id_outV_prop = self._gremlin(
                "g.V().has('id',"
                + vert_str
                + ").inE().outV().order().by('id',incr).valueMap()"
            )
            _process_node(list_id_outV, list_id_outV_val, list_id_outV_prop)
            # edge
            list_edge = self._gremlin(
                "g.V().has('id'," + vert_str + ").union(outE(), inE())"
            )
            _process_edge(list_edge)

        data_dict = {}
        data_dict["graphVisId"] = "0"
        data_dict["nodes"] = nodes
        data_dict["edges"] = edges

        return data_dict

    def queryGraphData(self, vertices, hop, interactive_query=None):
        """
        Set JSON value to `data` after query gremlin server.

        Args:
            vertices (list): Vertex Id list.
            hop (int): Number of top. Default to 1.
            interactive_query (:class:`InteractiveQuery`): Gremlin server instance.

        Returns: None
        """
        if not isinstance(vertices, (list, tuple, range)):
            vertices = [vertices]
        hop = int(hop)

        if interactive_query is not None:
            self._interactive_query = interactive_query

        if self._interactive_query is None:
            raise ValueError(
                "Failed to obtain interactive_query, unable to query data and draw graph."
            )

        if hop == 1:
            self.value_dict = self._process_vertices_1_hop(vertices)
            self.value = json.dumps(self.value_dict)
        else:
            raise NotImplementedError

    def queryNeighbor(self, ins, params, buffers):
        """
        Args:
            ins: Listening required.
            params (dict): Contains "degree" and "nodeId" of node.
            buffers (bool): Listening required.
        """
        if "nodeId" in params and "degree" in params:
            vid = str(params["nodeId"])
            # convert to original id
            oid = self._nodes_id_map[vid]
            hop = int(params["degree"])

            if hop == 1:
                new_value_dict = self._process_vertices_1_hop([oid])
                self.value_dict["nodes"].extend(new_value_dict["nodes"])
                self.value_dict["edges"].extend(new_value_dict["edges"])
                self.value = json.dumps(self.value_dict)
            else:
                raise NotImplementedError


def draw_graphscope_graph(graph, vertices, hop=1):
    """Visualize the graph data in the result cell when the draw functions are invoked

    Args:
        vertices (list): selected vertices.
        hop (int): draw induced subgraph with hop extension. Defaults to 1.

    Returns:
        A GraphModel.
    """
    graph._ensure_loaded()
    interactive_query = graph._session.gremlin(graph)

    gm = GraphModel()

    # for debugging
    # gm.addGraphFromData()

    gm.queryGraphData(vertices, hop, interactive_query)
    # listen on the 1~2 hops operation of node
    gm.on_msg(gm.queryNeighbor)

    return gm


def repr_graphscope_graph(graph, *args, **kwargs):
    from ipywidgets.widgets.widget import Widget

    if "_ipython_display_" in Widget.__dict__:
        return draw_graphscope_graph(graph, vertices=range(1, 100))._ipython_display_(
            *args, **kwargs
        )
    return draw_graphscope_graph(graph, vertices=range(1, 100))._repr_mimebundle_(
        *args, **kwargs
    )


def in_ipython():
    try:
        get_ipython().__class__.__name__
        return True
    except NameError:
        return False


def in_notebook():
    try:
        shell = get_ipython().__class__.__name__
        if shell == "ZMQInteractiveShell":
            return True  # Jupyter notebook or qtconsole
        if shell == "TerminalInteractiveShell":
            return False  # Terminal running IPython
        return False  # Other type (?)
    except NameError:
        return False  # Probably standard Python interpreter


def __graphin_for_graphscope(graphscope):
    if in_notebook():
        graph_type = getattr(graphscope, "Graph")
        setattr(graph_type, "draw", draw_graphscope_graph)
        from ipywidgets.widgets.widget import Widget

        if "_ipython_display_" in Widget.__dict__:
            setattr(graph_type, "_ipython_display_", repr_graphscope_graph)
        else:
            setattr(graph_type, "_repr_mimebundle_", repr_graphscope_graph)


def __register_graphin_for_graphscope():
    # if graphscope already loaded
    if "graphscope" in sys.modules:
        __graphin_for_graphscope(sys.modules["graphscope"])  # noqa: F821

    hookpoint = get_ipython().user_ns  # noqa: F821

    # added to graphscope extension lists
    if "__graphscope_extensions__" not in hookpoint:
        hookpoint["__graphscope_extensions__"] = []
    hookpoint["__graphscope_extensions__"].append(
        __graphin_for_graphscope  # noqa: F821
    )


if in_ipython():
    __register_graphin_for_graphscope()
del __graphin_for_graphscope
del __register_graphin_for_graphscope
