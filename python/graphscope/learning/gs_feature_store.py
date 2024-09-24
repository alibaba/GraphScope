import base64
import json
from multiprocessing.reduction import ForkingPickler
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple
from typing import Union

import numpy as np
import torch
from torch import Tensor
from torch_geometric.data import FeatureStore
from torch_geometric.data import TensorAttr
from torch_geometric.typing import FeatureTensorType

from graphscope.learning.graphlearn_torch.distributed.dist_client import request_server
from graphscope.learning.graphlearn_torch.distributed.dist_server import DistServer

KeyType = Union[Tuple[str, str, str], str]
IndexType = Union[torch.Tensor, np.ndarray, slice, int]


class GsFeatureStore(FeatureStore):
    def __init__(self, endpoints, handle=None, config=None, graph=None) -> None:
        super().__init__()
        self.handle = handle
        self.config = config
        self.attr2index: Dict[KeyType, Dict[str, int]] = {}
        self.tensor_attr_list: list[TensorAttr] = []
        if handle is not None:
            handle = json.loads(
                base64.b64decode(handle.encode("utf-8", errors="ignore")).decode(
                    "utf-8", errors="ignore"
                )
            )
            self.num_servers = handle["num_servers"]
        if config is not None:
            config = json.loads(
                base64.b64decode(config.encode("utf-8", errors="ignore")).decode(
                    "utf-8", errors="ignore"
                )
            )
            self.edge_features = config["edge_features"]
            self.node_features = config["node_features"]
            self.node_labels = config["node_labels"]
            self.edges = config["edges"]

            if self.node_features is not None:
                for node_type, node_features in self.node_features.items():
                    node_attr2index = {}
                    for idx, node_feature in enumerate(node_features):
                        self.tensor_attr_list.append(
                            TensorAttr(node_type, node_feature)
                        )
                        node_attr2index[node_feature] = idx
                    self.attr2index[node_type] = node_attr2index
            if self.edge_features is not None:
                for edge_type, edge_features in self.edge_features.items():
                    edge_attr2index = {}
                    for idx, edge_feature in enumerate(edge_features):
                        self.tensor_attr_list.append(
                            TensorAttr(edge_type, edge_feature)
                        )
                        edge_attr2index[edge_feature] = idx
                    self.attr2index[edge_type] = edge_attr2index

        self.endpoints = endpoints

    @staticmethod
    def key(attr: TensorAttr) -> KeyType:
        return (attr.group_name, attr.attr_name, attr.index)

    def _put_tensor(self, tensor: FeatureTensorType, attr: TensorAttr) -> bool:
        r"""To be implemented by :class:`GsFeatureStore`."""
        raise NotImplementedError

    def _get_tensor(self, attr: TensorAttr) -> Optional[Tensor]:
        group_name, attr_name, index = self.key(attr)
        result = None
        server_id = self._get_partition_id(attr)
        index = self.index_to_tensor(index)
        if isinstance(group_name, str):
            result = request_server(
                server_id, DistServer.get_node_feature, group_name, index
            )
        else:
            result = request_server(
                server_id, DistServer.get_edge_feature, group_name, index
            )
        if attr.is_set("attr_name"):
            attr_index = [self.attr2index[group_name][attr_name]]
            return result[0, attr_index]
        return result[0]

    def _get_partition_id(self, attr: TensorAttr) -> Optional[int]:
        result = None
        group_name, _, gid = self.key(attr)
        gid = self.index_to_tensor(gid)
        if isinstance(group_name, str):
            result = request_server(
                1, DistServer.get_node_partition_id, group_name, gid
            )
        else:
            result = request_server(
                1, DistServer.get_edge_partition_id, group_name, gid
            )
        return result

    def _remove_tensor(self, attr: TensorAttr) -> bool:
        r"""To be implemented by :class:`GsFeatureStore`."""
        raise NotImplementedError

    def _get_tensor_size(self, attr: TensorAttr) -> Optional[torch.Size]:
        group_name, attr_name, index = self.key(attr)
        if index is None:
            index = 0
            attr = TensorAttr(group_name, attr_name, index)
        tensor = self._get_tensor(attr)
        return tensor.shape

    def get_all_tensor_attrs(self) -> List[TensorAttr]:
        return self.tensor_attr_list

    def index_to_tensor(self, index: IndexType) -> torch.Tensor:
        if isinstance(index, torch.Tensor):
            return index
        elif isinstance(index, np.ndarray):
            return torch.from_numpy(index)
        elif isinstance(index, slice):
            start = index.start if index.start is not None else 0
            stop = index.stop if index.stop is not None else -1
            step = index.step if index.step is not None else 1
            return torch.arange(start, stop, step)
        elif isinstance(index, int):
            return torch.tensor([index])
        else:
            raise TypeError(f"Unsupported index type: {type(index)}")

    @classmethod
    def from_ipc_handle(cls, ipc_handle):
        return cls(*ipc_handle)

    def share_ipc(self):
        ipc_hanlde = (list(self.endpoints), self.handle, self.config)
        return ipc_hanlde


# Pickling Registration


def rebuild_featurestore(ipc_handle):
    fs = GsFeatureStore.from_ipc_handle(ipc_handle)
    return fs


def reduce_featurestore(FeatureStore: GsFeatureStore):
    ipc_handle = FeatureStore.share_ipc()
    return (rebuild_featurestore, (ipc_handle,))


ForkingPickler.register(GsFeatureStore, reduce_featurestore)
