from datetime import date, datetime  # noqa: F401

from typing import List, Dict  # noqa: F401

from gs_flex_coordinator.models.base_model import Model
from gs_flex_coordinator.models.edge_mapping import EdgeMapping
from gs_flex_coordinator.models.schema_mapping_loading_config import SchemaMappingLoadingConfig
from gs_flex_coordinator.models.vertex_mapping import VertexMapping
from gs_flex_coordinator import util

from gs_flex_coordinator.models.edge_mapping import EdgeMapping  # noqa: E501
from gs_flex_coordinator.models.schema_mapping_loading_config import SchemaMappingLoadingConfig  # noqa: E501
from gs_flex_coordinator.models.vertex_mapping import VertexMapping  # noqa: E501

class SchemaMapping(Model):
    """NOTE: This class is auto generated by OpenAPI Generator (https://openapi-generator.tech).

    Do not edit the class manually.
    """

    def __init__(self, graph=None, loading_config=None, vertex_mappings=None, edge_mappings=None):  # noqa: E501
        """SchemaMapping - a model defined in OpenAPI

        :param graph: The graph of this SchemaMapping.  # noqa: E501
        :type graph: str
        :param loading_config: The loading_config of this SchemaMapping.  # noqa: E501
        :type loading_config: SchemaMappingLoadingConfig
        :param vertex_mappings: The vertex_mappings of this SchemaMapping.  # noqa: E501
        :type vertex_mappings: List[VertexMapping]
        :param edge_mappings: The edge_mappings of this SchemaMapping.  # noqa: E501
        :type edge_mappings: List[EdgeMapping]
        """
        self.openapi_types = {
            'graph': str,
            'loading_config': SchemaMappingLoadingConfig,
            'vertex_mappings': List[VertexMapping],
            'edge_mappings': List[EdgeMapping]
        }

        self.attribute_map = {
            'graph': 'graph',
            'loading_config': 'loading_config',
            'vertex_mappings': 'vertex_mappings',
            'edge_mappings': 'edge_mappings'
        }

        self._graph = graph
        self._loading_config = loading_config
        self._vertex_mappings = vertex_mappings
        self._edge_mappings = edge_mappings

    @classmethod
    def from_dict(cls, dikt) -> 'SchemaMapping':
        """Returns the dict as a model

        :param dikt: A dict.
        :type: dict
        :return: The SchemaMapping of this SchemaMapping.  # noqa: E501
        :rtype: SchemaMapping
        """
        return util.deserialize_model(dikt, cls)

    @property
    def graph(self) -> str:
        """Gets the graph of this SchemaMapping.


        :return: The graph of this SchemaMapping.
        :rtype: str
        """
        return self._graph

    @graph.setter
    def graph(self, graph: str):
        """Sets the graph of this SchemaMapping.


        :param graph: The graph of this SchemaMapping.
        :type graph: str
        """

        self._graph = graph

    @property
    def loading_config(self) -> SchemaMappingLoadingConfig:
        """Gets the loading_config of this SchemaMapping.


        :return: The loading_config of this SchemaMapping.
        :rtype: SchemaMappingLoadingConfig
        """
        return self._loading_config

    @loading_config.setter
    def loading_config(self, loading_config: SchemaMappingLoadingConfig):
        """Sets the loading_config of this SchemaMapping.


        :param loading_config: The loading_config of this SchemaMapping.
        :type loading_config: SchemaMappingLoadingConfig
        """

        self._loading_config = loading_config

    @property
    def vertex_mappings(self) -> List[VertexMapping]:
        """Gets the vertex_mappings of this SchemaMapping.


        :return: The vertex_mappings of this SchemaMapping.
        :rtype: List[VertexMapping]
        """
        return self._vertex_mappings

    @vertex_mappings.setter
    def vertex_mappings(self, vertex_mappings: List[VertexMapping]):
        """Sets the vertex_mappings of this SchemaMapping.


        :param vertex_mappings: The vertex_mappings of this SchemaMapping.
        :type vertex_mappings: List[VertexMapping]
        """

        self._vertex_mappings = vertex_mappings

    @property
    def edge_mappings(self) -> List[EdgeMapping]:
        """Gets the edge_mappings of this SchemaMapping.


        :return: The edge_mappings of this SchemaMapping.
        :rtype: List[EdgeMapping]
        """
        return self._edge_mappings

    @edge_mappings.setter
    def edge_mappings(self, edge_mappings: List[EdgeMapping]):
        """Sets the edge_mappings of this SchemaMapping.


        :param edge_mappings: The edge_mappings of this SchemaMapping.
        :type edge_mappings: List[EdgeMapping]
        """

        self._edge_mappings = edge_mappings
