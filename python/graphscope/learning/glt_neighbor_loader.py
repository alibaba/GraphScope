from typing import Any
from typing import Dict
from typing import Iterator
from typing import List
from typing import Optional
from typing import Tuple
from typing import Union

from torch_geometric.data import Data
from torch_geometric.data import FeatureStore
from torch_geometric.data import GraphStore
from torch_geometric.data import HeteroData
from torch_geometric.loader.node_loader import NodeLoader
from torch_geometric.typing import InputNodes

from graphscope.learning.pyg_neighbor_sampler import PygNeighborSampler


class GltNeighborLoader(NodeLoader):
    def __init__(
        self,
        data: Union[Data, HeteroData, Tuple[FeatureStore, GraphStore]],
        input_nodes: InputNodes = None,
        neighbor_sampler: Optional[PygNeighborSampler] = None,
        **kwargs,
    ):
        if neighbor_sampler is None:
            raise ValueError("neighbor_sampler must be provided.")

        super().__init__(
            data=data,
            node_sampler=neighbor_sampler,
            input_nodes=input_nodes,
            **kwargs,
        )
        self.node_sampler = neighbor_sampler

    def __call__(self) -> Union[Data, HeteroData]:
        r"""Samples a subgraph from a batch of input nodes."""
        out = self.collate_fn()
        return out

    def collate_fn(self, data=None) -> Any:
        r"""Samples a subgraph from a batch of input nodes."""

        out = self.node_sampler.sample_from_nodes()

        return out

    def _get_iterator(self) -> Iterator:
        return super(NodeLoader, self)._get_iterator()
