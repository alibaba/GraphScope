from typing import Callable
from typing import Dict
from typing import List
from typing import Literal
from typing import Optional
from typing import Tuple
from typing import Union

import torch
from torch_geometric.data import Data
from torch_geometric.data import FeatureStore
from torch_geometric.data import GraphStore
from torch_geometric.data import HeteroData
from torch_geometric.sampler import BaseSampler
from torch_geometric.sampler.base import DataType
from torch_geometric.sampler.base import NumNeighbors
from torch_geometric.typing import EdgeType
from torch_geometric.typing import NodeType

from graphscope.learning.graphlearn_torch.distributed.dist_neighbor_loader import \
    DistNeighborLoader
from graphscope.learning.graphlearn_torch.distributed.dist_options import \
    AllDistSamplingWorkerOptions
from graphscope.learning.graphlearn_torch.distributed.dist_options import \
    RemoteDistSamplingWorkerOptions

NumNeighborsType = Union[NumNeighbors, List[int], Dict[EdgeType, List[int]]]
Seeds = Union[torch.Tensor, str] 
InputNodes = Union[Seeds, NodeType, Tuple[NodeType, Seeds], Tuple[NodeType, List[Seeds]]]
EdgeIndexTensor = Union[torch.Tensor, Tuple[torch.Tensor, torch.Tensor]]
InputEdges = Union[EdgeIndexTensor, EdgeType, Tuple[EdgeType, EdgeIndexTensor]]


class PygNeighborSampler(BaseSampler):
    def __init__(
        self,
        data: Union[Data, HeteroData, Tuple[FeatureStore, GraphStore]],
        data_split: Literal['train', 'valid', 'test'],
        num_neighbors: NumNeighborsType,
        
        # glt parameters:
        batch_size: int = 1,
        shuffle: bool = False,
        drop_last: bool = False,
        with_edge: bool = False,
        with_weight: bool = False,
        edge_dir: Literal['in', 'out'] = 'out',
        collect_features: bool = False,
        to_device: Optional[torch.device] = None,
        random_seed: Optional[int] = None,
        graphlearn_torch_loader: Optional[DistNeighborLoader] = None,
        
        # Sampling worker Options:
        server_rank: Optional[Union[int, List[int]]] = None,
        num_workers: int = 1,
        worker_devices: Optional[List[torch.device]] = None,
        worker_concurrency: int = 4,
        master_addr: Optional[str] = None,
        master_port: Optional[Union[str, int]] = None,
        num_rpc_threads: Optional[int] = None,
        rpc_timeout: float = 180,
        buffer_size: Optional[Union[int, str]] = None,
        prefetch_size: int = 4,
        worker_key: str = None,
        glt_graph = None,
        workload_type: Optional[Literal['train', 'validate', 'test']] = None,
    ):
        self.data_type = DataType.from_data(data)

        if self.data_type != DataType.remote:
            raise TypeError(
                f"'{self.__class__.__name__}' only supports remote data "
                f"loading, but got '{self.data_type}'")

        else:  # self.data_type == DataType.remote
            _, graph_store = data
            
            if data_split == 'train':
                master_port = graph_store._train_loader_master_port
            elif data_split == 'valid':
                master_port = graph_store._val_loader_master_port
            elif data_split == 'test':
                master_port = graph_store._test_loader_master_port
            else:
                raise ValueError(f"master_port is None and data_split is not valid: {data_split}")
            worker_key = str(master_port)

            if graphlearn_torch_loader == None:
                from graphscope.learning.gl_torch_graph import GLTorchGraph
                glt_graph = GLTorchGraph(graph_store.endpoints)
                self.glt_data_loader = DistNeighborLoader(
                    data=None,
                    num_neighbors=num_neighbors,
                    input_nodes=data_split,
                    batch_size=batch_size,
                    shuffle=shuffle,
                    collect_features=collect_features,
                    to_device=to_device,
                    drop_last=drop_last,
                    with_edge=with_edge,
                    with_weight=with_weight,
                    edge_dir=edge_dir,
                    random_seed=random_seed,
                    worker_options=RemoteDistSamplingWorkerOptions(
                        server_rank=server_rank,
                        num_workers=num_workers,
                        worker_devices=worker_devices,
                        worker_concurrency=worker_concurrency,
                        master_addr=master_addr,
                        master_port=master_port,
                        num_rpc_threads=num_rpc_threads,
                        rpc_timeout=rpc_timeout,
                        buffer_size=buffer_size,
                        prefetch_size=prefetch_size,
                        worker_key=worker_key,
                        glt_graph=glt_graph,
                        workload_type=workload_type,
                    ),
                )
            else:
                self.glt_data_loader = graphlearn_torch_loader
            self.glt_data_loader_iter = iter(self.glt_data_loader)
                

    @property
    def num_neighbors(self) -> NumNeighbors:
        return self._num_neighbors

    @num_neighbors.setter
    def num_neighbors(self, num_neighbors: NumNeighborsType):
        if isinstance(num_neighbors, NumNeighbors):
            self._num_neighbors = num_neighbors
        else:
            self._num_neighbors = NumNeighbors(num_neighbors)

    @property
    def is_hetero(self) -> bool:
        if self.data_type == DataType.homogeneous:
            return False
        if self.data_type == DataType.heterogeneous:
            return True

        # self.data_type == DataType.remote
        return self.edge_types != [None]

    @property
    def is_temporal(self) -> bool:
        return self.node_time is not None or self.edge_time is not None

    @property
    def disjoint(self) -> bool:
        return self._disjoint or self.is_temporal

    @disjoint.setter
    def disjoint(self, disjoint: bool):
        self._disjoint = disjoint

    # Node-based sampling #####################################################

    def sample_from_nodes(
        self
    ) -> Union[Data, HeteroData]:
        out = node_sample(self._sample)
        return out

    def _sample(
        self
    ) -> Union[Data, HeteroData]:
        r"""Implements neighbor sampling by calling either :obj:`pyg-lib` (if
        installed) or :obj:`torch-sparse` (if installed) sampling routines.
        """
        try:
            out = next(self.glt_data_loader_iter)
            return out
        except StopIteration:
            self.glt_data_loader_iter = iter(self.glt_data_loader)
            raise StopIteration
        


# Sampling Utilities ##########################################################


def node_sample(
    sample_fn: Callable
) -> Union[Data, HeteroData]:
    r"""Performs sampling from a :class:`NodeSamplerInput`, leveraging a
    sampling function that accepts a seed and (optionally) a seed time as
    input. Returns the output of this sampling procedure.
    """
    out = sample_fn()

    return out
