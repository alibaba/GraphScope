import ipywidgets as widgets
from spectate import mvc
from traitlets import TraitType
from traitlets import Unicode

from ._frontend import module_name
from ._frontend import module_version

import json


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
    """ Graph Widget """

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

    # 测试使用的文本值
    value = Unicode("").tag(sync=True)

    _interactive_query = None

    # 通过外部传入的数据渲染图
    def addGraphFromData(self, data):
        # nodeList = []
        # edgeList = []
        # def _addNodes(nodes):
        #     for node in nodes:
        #         current = {}
        #         current['id'] = str(node.id)
        #         current['label'] = node.label
        #         current["parentId"] = ""
        #         current["level"] = 0
        #         current["degree"] = 1  # need to update
        #         current["count"] = 0
        #         current["nodeType"] = node.nodeType
        #         current["properties"] = {}
        #         nodeList.append(current)

        # def _addEdges(list_edge):
        #     for line in list_edge:
        #         edge = {}
        #         edge["id"] = str(line.id)
        #         edge["label"] = line.label
        #         edge["source"] = str(line.outV.id)
        #         edge["target"] = str(line.inV.id)
        #         edge["count"] = 0
        #         edge["edgeType"] = line.edgeType
        #         edge["properties"] = {}
        #         edgeList.append(edge)

        # _addNodes(data['nodes'])
        # _addEdges(data['edges'])

        # data_dict = {}
        # data_dict["graphVisId"] = "0"
        # data_dict["nodes"] = nodeList
        # data_dict["edges"] = edgeList

        data = {
            'nodes': [
                    {
                        'id': 'node1',
                        'label': 'node1'
                    },
                    {
                        'id': 'node2',
                        'label': 'node12'
                    }
                ],
                'edges': [
                    {
                        'source': 'node1',
                        'target': 'node2'
                    }
                ]
        }
        
        data_str = json.dumps(data)
        self.value = data_str
    # 查询图数据
    def queryGraphData(self, vertices, hop, interactive_query=None, dump_to_file=False):
        # 调用 python 接口，获取查询的图数据，格式为 JSON，同时修改 data 值
        # self.value = 'result data'
        print("vertices", vertices)
        print("hop", hop)

        if interactive_query is not None:
            _interactive_query = interactive_query
        if _interactive_query is None:
            raise ValueError(
                "Failed to obtain interactive_query, unable to query data and draw graph."
            )
        #
        nodes_id_dict = {}
        edges_id_dict = {}
        nodes = []
        edges = []

        def _process_node(list_id, list_val, list_prop):
            for i in range(len(list_id)):
                node = {}
                node["id"] = str(list_id[i].id)
                #
                if node["id"] in nodes_id_dict:
                    continue
                #
                node["parentId"] = ""
                node["label"] = str(list_id[i].label)
                node["level"] = 0
                node["degree"] = 1  # need to update
                node["count"] = 0
                node["nodeType"] = ""
                node["properties"] = list_prop[i]
                nodes_id_dict[node["id"]] = True
                nodes.append(node)

        def _process_edge(list_edge):
            for line in list_edge:
                edge = {}
                edge["id"] = str(line.id)
                #
                if edge["id"] in edges_id_dict:
                    continue
                #
                edge["label"] = line.label
                edge["source"] = str(line.outV.id)
                edge["target"] = str(line.inV.id)
                edge["count"] = 0
                edge["edgeType"] = ""
                edge["properties"] = {}
                edges_id_dict[edge["id"]] = True
                edges.append(edge)

        def _gremlin(query=""):
            return _interactive_query.execute(query).all().result()

        def _process_vertices_1_hop(vertices):
            for vert in vertices:
                vert_str = str(vert)
                # node
                list_id = _gremlin("g.V().has('id'," + vert_str + ")")
                list_id_val = _gremlin("g.V().has('id'," + vert_str + ").values('id')")
                list_id_prop = _gremlin("g.V().has('id'," + vert_str + ").valueMap()")
                _process_node(list_id, list_id_val, list_id_prop)
                #
                list_id_inV = _gremlin(
                    "g.V().has('id',"
                    + vert_str
                    + ").outE().inV().order().by('id',incr)"
                )
                list_id_inV_val = _gremlin(
                    "g.V().has('id',"
                    + vert_str
                    + ").outE().inV().order().by('id',incr).values('id')"
                )
                list_id_inV_prop = _gremlin(
                    "g.V().has('id',"
                    + vert_str
                    + ").outE().inV().order().by('id',incr).valueMap()"
                )
                _process_node(list_id_inV, list_id_inV_val, list_id_inV_prop)
                #
                list_id_outV = _gremlin(
                    "g.V().has('id',"
                    + vert_str
                    + ").inE().outV().order().by('id',incr)"
                )
                list_id_outV_val = _gremlin(
                    "g.V().has('id',"
                    + vert_str
                    + ").inE().outV().order().by('id',incr).values('id')"
                )
                list_id_outV_prop = _gremlin(
                    "g.V().has('id',"
                    + vert_str
                    + ").inE().outV().order().by('id',incr).valueMap()"
                )
                _process_node(list_id_outV, list_id_outV_val, list_id_outV_prop)
                # edge
                list_edge = _gremlin(
                    "g.V().has('id'," + vert_str + ").union(outE(), inE())"
                )
                _process_edge(list_edge)
            #
            data_dict = {}
            data_dict["graphVisId"] = "0"
            data_dict["nodes"] = nodes
            data_dict["edges"] = edges
            #
            if dump_to_file:
                with open("datas.json", "w") as f:
                    json.dump(data_dict, f)
            #
            data_str = json.dumps(data_dict)
            self.value = data_str

        _process_vertices_1_hop(vertices)
