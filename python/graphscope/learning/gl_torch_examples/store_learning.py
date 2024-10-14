import base64
import json
from multiprocessing.reduction import ForkingPickler
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple

from torch_geometric.data.graph_store import EdgeAttr
from torch_geometric.data.graph_store import GraphStore
from torch_geometric.typing import EdgeTensorType

from graphscope.learning.graphlearn_torch.distributed.dist_client import request_server
from graphscope.learning.graphlearn_torch.distributed.dist_server import DistServer


class GsGraphStore(GraphStore):
    def __init__(self, endpoints, handle=None, config=None, graph=None) -> None:
        super().__init__()
        self.handle = handle
        self.config = config
        self.edge_attrs: Dict[Tuple[Tuple[str, str, str], str, bool], EdgeAttr] = {}

        if config is not None:
            config = json.loads(
                base64.b64decode(config.encode("utf-8", errors="ignore")).decode(
                    "utf-8", errors="ignore"
                )
            )
            self.edges = config["edges"]
            self.edge_weights = config["edge_weights"]
            self.edge_dir = config["edge_dir"]
            self.random_node_split = config["random_node_split"]
            if self.edges is not None:
                for edge in self.edges:
                    edge = tuple(edge)
                    if self.edge_dir is not None:
                        layout = "csr" if self.edge_dir == "out" else "csc"
                        is_sorted = False if layout == "csr" else True
                        self.edge_attrs[(edge, layout, is_sorted)] = EdgeAttr(
                            edge, layout, is_sorted
                        )
                    else:
                        layout = "coo"
                        self.edge_attrs[(edge, layout, False)] = EdgeAttr(
                            edge, layout, is_sorted
                        )

        assert len(endpoints) == 4
        self.endpoints = endpoints
        self._master_addr, self._server_client_master_port = endpoints[0].split(":")
        self._train_master_addr, self._train_loader_master_port = endpoints[1].split(
            ":"
        )
        self._val_master_addr, self._val_loader_master_port = endpoints[2].split(":")
        self._test_master_addr, self._test_loader_master_port = endpoints[3].split(":")
        assert (
            self._master_addr
            == self._train_master_addr
            == self._val_master_addr
            == self._test_master_addr
        )

    @property
    def master_addr(self):
        return self._master_addr

    @property
    def train_master_addr(self):
        return self._train_master_addr

    @property
    def val_master_addr(self):
        return self._val_master_addr

    @property
    def test_master_addr(self):
        return self._test_master_addr

    @property
    def server_client_master_port(self):
        return self._server_client_master_port

    @property
    def train_loader_master_port(self):
        return self._train_loader_master_port

    @property
    def val_loader_master_port(self):
        return self._val_loader_master_port

    @property
    def test_loader_master_port(self):
        return self._test_loader_master_port

    def get_handle(self):
        return self.handle

    def get_config(self):
        return self.config

    def get_endpoints(self):
        return self.endpoints

    @staticmethod
    def key(attr: EdgeAttr) -> Tuple:
        return (attr.edge_type, attr.layout.value, attr.is_sorted, attr.size)

    def _put_edge_index(
        self,
        edge_index: EdgeTensorType,
        edge_attr: EdgeAttr,
    ) -> bool:
        r"""To be implemented by :class:`GsFeatureStore`."""
        raise NotImplementedError

    def _get_edge_index(self, edge_attr: EdgeAttr) -> Optional[EdgeTensorType]:
        r"""Obtains a :class:`EdgeTensorType` from the remote server with :class:`EdgeAttr`.

        Args:
            edge_attr(`EdgeAttr`): Uniquely corresponds to a topology of subgraph .

        Returns:
            edge_index(`EdgeTensorType`): The edge index tensor, which is a :class:`tuple` of\
            (row indice tensor, column indice tensor)(COO)\
            (row ptr tensor, column indice tensor)(CSR)\
            (column ptr tensor, row indice tensor)(CSC).
        """
        group_name, layout, is_sorted, _ = self.key(edge_attr)
        edge_index = None
        edge_index, size = request_server(
            0, DistServer.get_edge_index, group_name, layout
        )
        if edge_index is not None:
            new_edge_attr = EdgeAttr(group_name, layout, is_sorted, size)
            self.edge_attrs[(group_name, layout, is_sorted)] = new_edge_attr
        return edge_index

    def _remove_edge_index(self, edge_attr: EdgeAttr) -> bool:
        r"""To be implemented by :class:`GsFeatureStore`."""
        raise NotImplementedError

    def get_all_edge_attrs(self) -> List[EdgeAttr]:
        r"""Obtains all the subgraph type stored in remote server.

        Returns:
            edge_attrs(`List[EdgeAttr]`): All the subgraph type stored in the remote server.
        """
        result = []
        for attr in self.edge_attrs.values():
            if attr.size is None:
                self._get_edge_index(attr)
                result.append(
                    self.edge_attrs[(attr.edge_type, attr.layout.value, attr.is_sorted)]
                )
            else:
                result.append(attr)
        return result

    @classmethod
    def from_ipc_handle(cls, ipc_handle):
        return cls(*ipc_handle)

    def share_ipc(self):
        ipc_hanlde = (list(self.endpoints), self.handle, self.config)
        return ipc_hanlde


# Pickling Registration


def rebuild_graphstore(ipc_handle):
    gs = GsGraphStore.from_ipc_handle(ipc_handle)
    return gs


def reduce_graphstore(GraphStore: GsGraphStore):
    ipc_handle = GraphStore.share_ipc()
    return (rebuild_graphstore, (ipc_handle,))


ForkingPickler.register(GsGraphStore, reduce_graphstore)