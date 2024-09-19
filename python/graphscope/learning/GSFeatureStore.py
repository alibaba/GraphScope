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


class GSFeatureStore(FeatureStore):
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
        r"""To be implemented by :class:`GSFeatureStore`."""
        raise NotImplementedError

    def _get_tensor(self, attr: TensorAttr) -> Optional[Tensor]:
        r"""To be implemented by :class:`GSFeatureStore`."""
        raise NotImplementedError

    def _remove_tensor(self, attr: TensorAttr) -> bool:
        r"""To be implemented by :class:`GSFeatureStore`."""
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
                    TensorAttrList.append(TensorAttr(node_type, node_feature, torch.tensor([idx])))
        if self.edge_features is not None:
            for edge_type, edge_features in self.edge_features.items():
                for idx, edge_feature in enumerate(edge_features):
                    TensorAttrList.append(TensorAttr(edge_type, edge_feature, torch.tensor([idx])))
        return TensorAttrList
    
    def _build_features(
            self, 
            feature_data, 
            id2idx, 
            split_ratio: Union[float, Dict[str, float]] = 0.0,
            device_group_list: Optional[List[DeviceGroup]] = None,
            device: Optional[int] = None,
            with_gpu: bool = True,
            dtype: Optional[torch.dtype] = None
        ):
        r""" Build `Feature`s for node/edge feature data.
        """
        if feature_data is not None:
            if isinstance(feature_data, dict):
                # heterogeneous.
                if not isinstance(split_ratio, dict):
                    split_ratio = {
                        graph_type: float(split_ratio)
                        for graph_type in feature_data.keys()
                    }

                if id2idx is not None:
                    assert isinstance(id2idx, dict)
                else:
                    id2idx = {}

                features = {}
                for graph_type, feat in feature_data.items():
                    features[graph_type] = Feature(
                        feat, id2idx.get(graph_type, None),
                        split_ratio.get(graph_type, 0.0),
                        device_group_list, device, with_gpu,
                        dtype if dtype is not None else feat.dtype
                    )
            else:
                # homogeneous.
                features = Feature(
                    feature_data, id2idx, float(split_ratio),
                    device_group_list, device, with_gpu,
                    dtype if dtype is not None else feature_data.dtype
                )
        else:
            features = None

        return features
    
    @classmethod
    def from_ipc_handle(cls, ipc_handle):
        return cls(*ipc_handle)

    def share_ipc(self):
        ipc_hanlde = (
            list(self.endpoints), self.handle, self.config
        )
        return ipc_hanlde


## Pickling Registration

def rebuild_featurestore(ipc_handle):
    fs = GSFeatureStore.from_ipc_handle(ipc_handle)
    return fs

def reduce_featurestore(FeatureStore: GSFeatureStore):
    ipc_handle = FeatureStore.share_ipc()
    return (rebuild_featurestore, (ipc_handle, ))

ForkingPickler.register(GSFeatureStore, reduce_featurestore)