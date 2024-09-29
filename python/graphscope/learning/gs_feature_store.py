import base64
import json
from dataclasses import dataclass
from enum import Enum
from multiprocessing.reduction import ForkingPickler
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple
from typing import Union

import numpy as np
import torch
from torch import Tensor
from torch_geometric.data.feature_store import FeatureStore
from torch_geometric.data.feature_store import IndexType
from torch_geometric.data.feature_store import TensorAttr
from torch_geometric.data.feature_store import _FieldStatus
from torch_geometric.typing import FeatureTensorType

from graphscope.learning.graphlearn_torch.distributed.dist_client import request_server
from graphscope.learning.graphlearn_torch.distributed.dist_server import DistServer
from graphscope.learning.graphlearn_torch.typing import EdgeType
from graphscope.learning.graphlearn_torch.typing import NodeType

KeyType = Tuple[str, ...]

@dataclass
class GsTensorAttr(TensorAttr):
    group_name: Optional[Union[NodeType, EdgeType]] = _FieldStatus.UNSET
    is_label: Optional[bool] = False


class GsFeatureStore(FeatureStore):
    def __init__(self, endpoints, handle=None, config=None, graph=None) -> None:
        super().__init__()
        self.handle = handle
        self.config = config
        self.attr2index: Dict[KeyType, Dict[str, int]] = {}
        self.tensor_attrs: Dict[Tuple[Union[NodeType, EdgeType], str], GsTensorAttr] = {}
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

            if self.node_features is not None:
                for node_type, node_features in self.node_features.items():
                    node_attr2index = {}
                    for idx, node_feature in enumerate(node_features):
                        self.tensor_attrs[(node_type, node_feature)] = GsTensorAttr(node_type, node_feature)
                        node_attr2index[node_feature] = idx
                    self.attr2index[node_type] = node_attr2index
            if self.edge_features is not None:
                for edge_type, edge_features in self.edge_features.items():
                    edge_attr2index = {}
                    for idx, edge_feature in enumerate(edge_features):
                        self.tensor_attrs[(edge_type, edge_feature)] = GsTensorAttr(edge_type, edge_feature)
                        edge_attr2index[edge_feature] = idx
                    self.attr2index[edge_type] = edge_attr2index
            if self.node_labels is not None:
                for node_type, node_label in self.node_labels.items():
                    self.tensor_attrs[(node_type, node_label)] = GsTensorAttr(node_type, node_label, is_label=True)
                    if node_type in self.attr2index:
                        _attr2index = self.attr2index[node_type]
                        _attr2index[node_label] = 0
                        self.attr2index[node_type] = _attr2index
                    else:
                        label_attr2index = {}
                        label_attr2index[node_feature] = 0
                        self.attr2index[node_type] = label_attr2index

        self.endpoints = endpoints

    @staticmethod
    def key(attr: GsTensorAttr) -> KeyType:
        return (attr.group_name, attr.attr_name, attr.index, attr.is_label)

    def _put_tensor(self, tensor: FeatureTensorType, attr: GsTensorAttr) -> bool:
        r"""To be implemented by :class:`GsFeatureStore`."""
        raise NotImplementedError

    def _get_tensor(self, attr: GsTensorAttr) -> Optional[Tensor]:
        group_name, attr_name, index, is_label = self.key(attr)
        if not self._check_attr(attr):
            raise ValueError(f"Attribute {group_name}-{attr_name} not found in feature store.")
        result = None
        server_id = self._get_partition_id(attr)
        index = self.index_to_tensor(index)
        if is_label:
            result = request_server(
                server_id, DistServer.get_node_label, group_name, index
            )
        else:
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

    def _get_partition_id(self, attr: GsTensorAttr) -> Optional[int]:
        result = None
        group_name, _, gid, _ = self.key(attr)
        gid = self.index_to_tensor(gid)
        if isinstance(group_name, str):
            result = request_server(
                0, DistServer.get_node_partition_id, group_name, gid
            )
        else:
            result = request_server(
                0, DistServer.get_edge_partition_id, group_name, gid
            )
        return result

    def _remove_tensor(self, attr: GsTensorAttr) -> bool:
        r"""To be implemented by :class:`GsFeatureStore`."""
        raise NotImplementedError
    
    def _check_attr(self, attr: GsTensorAttr) -> bool:
        group_name, attr_name, _, _ = self.key(attr)
        return (group_name, attr_name) in self.tensor_attrs

    def _get_tensor_size(self, attr: GsTensorAttr) -> Optional[torch.Size]:
        group_name, attr_name, _, _ = self.key(attr)
        attr = GsTensorAttr(group_name, attr_name, 0)
        tensor = self._get_tensor(attr)
        return tensor.shape

    def get_all_tensor_attrs(self) -> List[GsTensorAttr]:
        return [attr for attr in self.tensor_attrs.values()]

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
