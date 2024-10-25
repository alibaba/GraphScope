import base64
import json
from collections import defaultdict
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
from graphscope.learning.graphlearn_torch.typing import NodeType

KeyType = Tuple[str, ...]


class GsFeatureStore(FeatureStore):
    def __init__(self, config) -> None:
        super().__init__()
        self.config = config
        self.tensor_attrs: Dict[Tuple[NodeType, str], TensorAttr] = {}

        assert config is not None
        config = json.loads(
            base64.b64decode(config.encode("utf-8", errors="ignore")).decode(
                "utf-8", errors="ignore"
            )
        )
        self.node_features = config["node_features"]
        self.node_labels = config["node_labels"]
        self.edges = config["edges"]

        assert self.node_features is not None
        self.node_types = set()
        for node in self.node_features:
            self.node_types.add(node)

        for edge in self.edges:
            self.node_types.add(edge[0])
            self.node_types.add(edge[-1])

        for node_type in self.node_types:
            self.tensor_attrs[(node_type, "x")] = TensorAttr(node_type, "x")

        assert self.node_labels is not None
        for node_type, node_label in self.node_labels.items():
            self.tensor_attrs[(node_type, node_label)] = TensorAttr(
                node_type, node_label
            )

    @staticmethod
    def key(attr: TensorAttr) -> KeyType:
        return (attr.group_name, attr.attr_name, attr.index)

    def _put_tensor(self, tensor: FeatureTensorType, attr: TensorAttr) -> bool:
        r"""To be implemented by :class:`GsFeatureStore`."""
        raise NotImplementedError

    def _get_tensor(self, attr: TensorAttr) -> Optional[Tensor]:
        r"""Obtains a :class:`torch.Tensor` from the remote server.

        Args:
            attr(`TensorAttr`): Uniquely corresponds to a node/edge feature tensor .

        Raises:
            ValueError: If the attr can not be found in the attrlists of feature store.

        Returns:
            feature(`torch.Tensor`): The node/edge feature tensor.
        """

        group_name, attr_name, index = self.key(attr)
        if not self._check_attr(attr):
            raise ValueError(
                f"Attribute {group_name}-{attr_name} not found in feature store."
            )
        result = torch.tensor([])
        index = self.index_to_tensor(index)
        if index.numel() == 0:
            return result

        server_id = self._get_partition_id(attr)

        partition_to_ids = defaultdict(list)
        partition_to_indices = defaultdict(list)
        for idx, id in enumerate(index):
            partition_id = server_id[idx].item()
            partition_to_ids[partition_id].append(id.item())
            partition_to_indices[partition_id].append(idx)

        server_fun = None
        labels = self.node_labels[group_name]
        is_label = False
        if isinstance(labels, list):
            is_label = attr_name in labels
        elif isinstance(labels, str):
            is_label = attr_name == labels

        if is_label:
            server_fun = DistServer.get_node_label
        else:
            server_fun = DistServer.get_node_feature

        partition_to_features = defaultdict(list)
        for partition_id, ids in partition_to_ids.items():
            feature = request_server(
                partition_id, server_fun, group_name, self.index_to_tensor(ids)
            )
            partition_to_features[partition_id] = feature

        result = torch.zeros(
            (len(index), list(partition_to_features.values())[0].shape[-1])
        )
        for partition_id, indices in partition_to_indices.items():
            partition_features = partition_to_features[partition_id]
            for i, idx in enumerate(indices):
                result[idx] = partition_features[i]

        if is_label:
            result = result.reshape(-1)
        return result

    def _get_partition_id(self, attr: TensorAttr) -> Optional[int]:
        r"""Obtains the id of the partition where the tensor is stored from remote server.

        Args:
            attr(`TensorAttr`): Uniquely corresponds to a node/edge feature tensor .

        Returns:
            partition_id(int): The corresponding partition id.
        """
        result = None
        group_name, _, gid = self.key(attr)
        gid = self.index_to_tensor(gid)
        result = request_server(0, DistServer.get_node_partition_id, group_name, gid)
        return result

    def _remove_tensor(self, attr: TensorAttr) -> bool:
        r"""To be implemented by :class:`GsFeatureStore`."""
        raise NotImplementedError

    def _check_attr(self, attr: TensorAttr) -> bool:
        r"""Check the given :class:`TensorAttr` is stored in remote server or not.

        Args:
            attr(`TensorAttr`): Uniquely corresponds to a node/edge feature tensor .

        Returns:
            flag(bool): True: :class:`TensorAttr` is stored in remote server. \
                False: :class:`TensorAttr` is not stored in remote server
        """
        group_name, attr_name, _ = self.key(attr)
        if not attr.is_set("attr_name"):
            return any(group_name in key for key in self.tensor_attrs.keys())
        return (group_name, attr_name) in self.tensor_attrs

    def _get_tensor_size(self, attr: TensorAttr) -> Optional[torch.Size]:
        r"""Obtains the dimension of feature tensor from remote server.

        Args:
            attr(`TensorAttr`): Uniquely corresponds to a node/edge feature tensor type .

        Returns:
            tensor_size(`torch.Size`): The dimension of corresponding tensor type.
        """
        group_name, attr_name, _ = self.key(attr)
        attr = TensorAttr(group_name, attr_name, 0)
        tensor = self._get_tensor(attr)
        return tensor[0].size()

    def get_all_tensor_attrs(self) -> List[TensorAttr]:
        r"""Obtains all the tensor type stored in remote server.

        Returns:
            tensor_attrs(`List[TensorAttr]`): All the tensor type stored in the remote server.
        """
        return [attr for attr in self.tensor_attrs.values()]

    def index_to_tensor(self, index) -> torch.Tensor:
        r"""Convert the Index to type :class:`torch.Tensor`.

        Args:
            index(`IndexType`): The index that needs to be converted.

        Returns:
            index(`torch.Tensor`): index of type :class:`torch.Tensor`.
        """
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
        elif isinstance(index, list):
            return torch.tensor(index)
        else:
            raise TypeError(f"Unsupported index type: {type(index)}")

    @classmethod
    def from_ipc_handle(cls, ipc_handle):
        return cls(*ipc_handle)

    def share_ipc(self):
        ipc_hanlde = self.config
        return ipc_hanlde


# Pickling Registration


def rebuild_featurestore(ipc_handle):
    fs = GsFeatureStore.from_ipc_handle(ipc_handle)
    return fs


def reduce_featurestore(FeatureStore: GsFeatureStore):
    ipc_handle = FeatureStore.share_ipc()
    return (rebuild_featurestore, (ipc_handle,))


ForkingPickler.register(GsFeatureStore, reduce_featurestore)
