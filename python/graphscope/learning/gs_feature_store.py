import base64
import json
from multiprocessing.reduction import ForkingPickler
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple
from typing import Union

import torch
from torch import Tensor
from torch_geometric.data import FeatureStore
from torch_geometric.data import TensorAttr
from torch_geometric.typing import FeatureTensorType

from graphscope.learning.graphlearn_torch.data import DeviceGroup
from graphscope.learning.graphlearn_torch.data import Feature

KeyType = Tuple[Optional[str], Optional[str]]


class GsFeatureStore(FeatureStore):
    def __init__(self, endpoints, handle=None, config=None, graph=None) -> None:
        super().__init__()
        # self.store: Dict[KeyType, Tuple[Tensor, Tensor]] = {}
        self.handle = handle
        self.config = config

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

        self.endpoints = endpoints

    @staticmethod
    def key(attr: TensorAttr) -> KeyType:
        return (attr.group_name, attr.attr_name)

    def _put_tensor(self, tensor: FeatureTensorType, attr: TensorAttr) -> bool:
        r"""To be implemented by :class:`GsFeatureStore`."""
        raise NotImplementedError

    def _get_tensor(self, attr: TensorAttr) -> Optional[Tensor]:
        r"""To be implemented by :class:`GsFeatureStore`."""
        raise NotImplementedError

    def _remove_tensor(self, attr: TensorAttr) -> bool:
        r"""To be implemented by :class:`GsFeatureStore`."""
        raise NotImplementedError

    def _get_tensor_size(self, attr: TensorAttr) -> Optional[Tuple[int, ...]]:
        if self.node_features is not None:
            node_tensor = self.node_features.get(attr.group_name)
            if node_tensor is not None:
                return [len(node_tensor)]
        if self.edge_features is not None:
            edge_tensor = self.edge_features.get(attr.group_name)
            if edge_tensor is not None:
                return [len(edge_tensor)]
        return None

    def get_all_tensor_attrs(self) -> List[TensorAttr]:
        TensorAttrList = []
        if self.node_features is not None:
            for node_type, node_features in self.node_features.items():
                for idx, node_feature in enumerate(node_features):
                    TensorAttrList.append(
                        TensorAttr(node_type, node_feature, torch.tensor([idx]))
                    )
        if self.edge_features is not None:
            for edge_type, edge_features in self.edge_features.items():
                for idx, edge_feature in enumerate(edge_features):
                    TensorAttrList.append(
                        TensorAttr(edge_type, edge_feature, torch.tensor([idx]))
                    )
        return TensorAttrList

    @classmethod
    def from_ipc_handle(cls, ipc_handle):
        return cls(*ipc_handle)

    def share_ipc(self):
        ipc_hanlde = (list(self.endpoints), self.handle, self.config)
        return ipc_hanlde


## Pickling Registration


def rebuild_featurestore(ipc_handle):
    fs = GsFeatureStore.from_ipc_handle(ipc_handle)
    return fs


def reduce_featurestore(FeatureStore: GsFeatureStore):
    ipc_handle = FeatureStore.share_ipc()
    return (rebuild_featurestore, (ipc_handle,))


ForkingPickler.register(GsFeatureStore, reduce_featurestore)
