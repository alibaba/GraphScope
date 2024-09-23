import base64
import json
from multiprocessing.reduction import ForkingPickler
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple

from torch_geometric.data import EdgeAttr
from torch_geometric.data import GraphStore
from torch_geometric.typing import EdgeTensorType


class GsGraphStore(GraphStore):
    def __init__(self, endpoints, handle=None, config=None, graph=None) -> None:
        super().__init__()
        # self.store: Dict[Tuple, Tuple[Tensor, Tensor]] = {}
        self.handle = handle
        self.config = config

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
        
        assert len(endpoints) == 4
        self.endpoints = endpoints
        self._master_addr, self._server_client_master_port = endpoints[0].split(":")
        self._train_master_addr, self._train_loader_master_port = endpoints[1].split(
            ":"
        )
        self._val_master_addr, self._val_loader_master_port = endpoints[2].split(":")
        self._test_master_addr, self._test_loader_master_port = endpoints[3].split(
            ":"
        )
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
        r"""To be implemented by :class:`GsFeatureStore`."""
        raise NotImplementedError

    def _remove_edge_index(self, edge_attr: EdgeAttr) -> bool:
        r"""To be implemented by :class:`GsFeatureStore`."""
        raise NotImplementedError

    def get_all_edge_attrs(self) -> List[EdgeAttr]:
        EdgeAttrList = []
        if self.edges is not None:
            for edge in self.edges:
                if self.edge_dir != None:
                    layout ="csr" if self.edge_dir == "out" else "csc"
                    is_sorted = False if layout == "csr" else True
                else:
                    layout = "coo"
                EdgeAttrList.append(EdgeAttr(edge, layout, is_sorted))
        return EdgeAttrList

    @classmethod
    def from_ipc_handle(cls, ipc_handle):
        return cls(*ipc_handle)

    def share_ipc(self):
        ipc_hanlde = (
            list(self.endpoints), self.handle, self.config
        )
        return ipc_hanlde


## Pickling Registration

def rebuild_graphstore(ipc_handle):
  gs = GsGraphStore.from_ipc_handle(ipc_handle)
  return gs

def reduce_graphstore(GraphStore: GsGraphStore):
  ipc_handle = GraphStore.share_ipc()
  return (rebuild_graphstore, (ipc_handle, ))

ForkingPickler.register(GsGraphStore, reduce_graphstore)