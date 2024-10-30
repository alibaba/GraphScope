import base64
import json
from multiprocessing.reduction import ForkingPickler
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple

import torch
from torch_geometric.data.graph_store import EdgeAttr
from torch_geometric.data.graph_store import GraphStore
from torch_geometric.typing import EdgeTensorType
from torch_geometric.utils import index_sort

from graphscope.learning.graphlearn_torch.distributed.dist_client import request_server
from graphscope.learning.graphlearn_torch.distributed.dist_server import DistServer


class GsGraphStore(GraphStore):
    def __init__(self, config) -> None:
        super().__init__()
        self.config = config
        self.edge_attrs: Dict[Tuple[Tuple[str, str, str], str, bool], EdgeAttr] = {}

        assert config is not None
        config = json.loads(
            base64.b64decode(config.encode("utf-8", errors="ignore")).decode(
                "utf-8", errors="ignore"
            )
        )
        self.edges = config["edges"]
        self.edge_dir = config["edge_dir"]

        assert self.edges is not None
        for edge in self.edges:
            edge = tuple(edge)
            # Only support COO layout
            layout = "coo"
            new_edge_attr = EdgeAttr(edge, layout, True)
            self.edge_attrs[(edge, layout, True)] = new_edge_attr

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
            (row indice tensor, column indice tensor)
        """
        group_name, layout, _, _ = self.key(edge_attr)
        num_servers, _, _, _ = request_server(0, DistServer.get_dataset_meta)
        rows = []
        cols = []
        for server_id in range(num_servers):
            (row, col) = request_server(
                server_id, DistServer.get_edge_index, group_name, layout
            )
            rows.append(row)
            cols.append(col)

        global_row = torch.cat(rows, dim=0)
        global_row, perm = index_sort(global_row, max_value=int(global_row.max()) + 1)
        global_col = torch.cat(cols, dim=0)[perm]
        return (global_row, global_col)

    def _remove_edge_index(self, edge_attr: EdgeAttr) -> bool:
        r"""To be implemented by :class:`GsFeatureStore`."""
        raise NotImplementedError

    def _get_edge_size(self, edge_attr: EdgeAttr) -> Tuple[int, int]:
        r"""Obtains a :class:`EdgeTensorType` from the remote server with :class:`EdgeAttr`.

        Args:
            edge_attr(`EdgeAttr`): Uniquely corresponds to a topology of subgraph .

        Returns:
            size(`tupple(int, int)`): The size of the subgraph.
        """
        group_name, layout, is_sorted, _ = self.key(edge_attr)
        (row, col) = self._get_edge_index(edge_attr)
        size = (int(row.max()) + 1, int(col.max()) + 1)
        new_edge_attr = EdgeAttr(group_name, layout, is_sorted, size)
        self.edge_attrs[(group_name, layout, is_sorted)] = new_edge_attr
        return size

    def get_all_edge_attrs(self) -> List[EdgeAttr]:
        r"""Obtains all the subgraph type stored in remote server.

        Returns:
            edge_attrs(`List[EdgeAttr]`): All the subgraph type stored in the remote server.
        """
        result = []
        for attr in self.edge_attrs.values():
            result.append(attr)
        return result

    @classmethod
    def from_ipc_handle(cls, ipc_handle):
        return cls(*ipc_handle)

    def share_ipc(self):
        ipc_hanlde = self.config
        return ipc_hanlde


# Pickling Registration


def rebuild_graphstore(ipc_handle):
    gs = GsGraphStore.from_ipc_handle(ipc_handle)
    return gs


def reduce_graphstore(GraphStore: GsGraphStore):
    ipc_handle = GraphStore.share_ipc()
    return (rebuild_graphstore, (ipc_handle,))


ForkingPickler.register(GsGraphStore, reduce_graphstore)
