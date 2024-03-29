from datetime import date, datetime  # noqa: F401

from typing import List, Dict  # noqa: F401

from gs_flex_coordinator.models.base_model import Model
from gs_flex_coordinator import util


class EdgeDataSource(Model):
    """NOTE: This class is auto generated by OpenAPI Generator (https://openapi-generator.tech).

    Do not edit the class manually.
    """

    def __init__(self, data_source=None, type_name=None, source_vertex=None, destination_vertex=None, location=None, source_pk_column_map=None, destination_pk_column_map=None, property_mapping=None):  # noqa: E501
        """EdgeDataSource - a model defined in OpenAPI

        :param data_source: The data_source of this EdgeDataSource.  # noqa: E501
        :type data_source: str
        :param type_name: The type_name of this EdgeDataSource.  # noqa: E501
        :type type_name: str
        :param source_vertex: The source_vertex of this EdgeDataSource.  # noqa: E501
        :type source_vertex: str
        :param destination_vertex: The destination_vertex of this EdgeDataSource.  # noqa: E501
        :type destination_vertex: str
        :param location: The location of this EdgeDataSource.  # noqa: E501
        :type location: str
        :param source_pk_column_map: The source_pk_column_map of this EdgeDataSource.  # noqa: E501
        :type source_pk_column_map: Dict[str, object]
        :param destination_pk_column_map: The destination_pk_column_map of this EdgeDataSource.  # noqa: E501
        :type destination_pk_column_map: Dict[str, object]
        :param property_mapping: The property_mapping of this EdgeDataSource.  # noqa: E501
        :type property_mapping: Dict[str, object]
        """
        self.openapi_types = {
            'data_source': str,
            'type_name': str,
            'source_vertex': str,
            'destination_vertex': str,
            'location': str,
            'source_pk_column_map': Dict[str, object],
            'destination_pk_column_map': Dict[str, object],
            'property_mapping': Dict[str, object]
        }

        self.attribute_map = {
            'data_source': 'data_source',
            'type_name': 'type_name',
            'source_vertex': 'source_vertex',
            'destination_vertex': 'destination_vertex',
            'location': 'location',
            'source_pk_column_map': 'source_pk_column_map',
            'destination_pk_column_map': 'destination_pk_column_map',
            'property_mapping': 'property_mapping'
        }

        self._data_source = data_source
        self._type_name = type_name
        self._source_vertex = source_vertex
        self._destination_vertex = destination_vertex
        self._location = location
        self._source_pk_column_map = source_pk_column_map
        self._destination_pk_column_map = destination_pk_column_map
        self._property_mapping = property_mapping

    @classmethod
    def from_dict(cls, dikt) -> 'EdgeDataSource':
        """Returns the dict as a model

        :param dikt: A dict.
        :type: dict
        :return: The EdgeDataSource of this EdgeDataSource.  # noqa: E501
        :rtype: EdgeDataSource
        """
        return util.deserialize_model(dikt, cls)

    @property
    def data_source(self) -> str:
        """Gets the data_source of this EdgeDataSource.


        :return: The data_source of this EdgeDataSource.
        :rtype: str
        """
        return self._data_source

    @data_source.setter
    def data_source(self, data_source: str):
        """Sets the data_source of this EdgeDataSource.


        :param data_source: The data_source of this EdgeDataSource.
        :type data_source: str
        """
        allowed_values = ["ODPS", "FILE"]  # noqa: E501
        if data_source not in allowed_values:
            raise ValueError(
                "Invalid value for `data_source` ({0}), must be one of {1}"
                .format(data_source, allowed_values)
            )

        self._data_source = data_source

    @property
    def type_name(self) -> str:
        """Gets the type_name of this EdgeDataSource.


        :return: The type_name of this EdgeDataSource.
        :rtype: str
        """
        return self._type_name

    @type_name.setter
    def type_name(self, type_name: str):
        """Sets the type_name of this EdgeDataSource.


        :param type_name: The type_name of this EdgeDataSource.
        :type type_name: str
        """

        self._type_name = type_name

    @property
    def source_vertex(self) -> str:
        """Gets the source_vertex of this EdgeDataSource.


        :return: The source_vertex of this EdgeDataSource.
        :rtype: str
        """
        return self._source_vertex

    @source_vertex.setter
    def source_vertex(self, source_vertex: str):
        """Sets the source_vertex of this EdgeDataSource.


        :param source_vertex: The source_vertex of this EdgeDataSource.
        :type source_vertex: str
        """

        self._source_vertex = source_vertex

    @property
    def destination_vertex(self) -> str:
        """Gets the destination_vertex of this EdgeDataSource.


        :return: The destination_vertex of this EdgeDataSource.
        :rtype: str
        """
        return self._destination_vertex

    @destination_vertex.setter
    def destination_vertex(self, destination_vertex: str):
        """Sets the destination_vertex of this EdgeDataSource.


        :param destination_vertex: The destination_vertex of this EdgeDataSource.
        :type destination_vertex: str
        """

        self._destination_vertex = destination_vertex

    @property
    def location(self) -> str:
        """Gets the location of this EdgeDataSource.


        :return: The location of this EdgeDataSource.
        :rtype: str
        """
        return self._location

    @location.setter
    def location(self, location: str):
        """Sets the location of this EdgeDataSource.


        :param location: The location of this EdgeDataSource.
        :type location: str
        """

        self._location = location

    @property
    def source_pk_column_map(self) -> Dict[str, object]:
        """Gets the source_pk_column_map of this EdgeDataSource.


        :return: The source_pk_column_map of this EdgeDataSource.
        :rtype: Dict[str, object]
        """
        return self._source_pk_column_map

    @source_pk_column_map.setter
    def source_pk_column_map(self, source_pk_column_map: Dict[str, object]):
        """Sets the source_pk_column_map of this EdgeDataSource.


        :param source_pk_column_map: The source_pk_column_map of this EdgeDataSource.
        :type source_pk_column_map: Dict[str, object]
        """

        self._source_pk_column_map = source_pk_column_map

    @property
    def destination_pk_column_map(self) -> Dict[str, object]:
        """Gets the destination_pk_column_map of this EdgeDataSource.


        :return: The destination_pk_column_map of this EdgeDataSource.
        :rtype: Dict[str, object]
        """
        return self._destination_pk_column_map

    @destination_pk_column_map.setter
    def destination_pk_column_map(self, destination_pk_column_map: Dict[str, object]):
        """Sets the destination_pk_column_map of this EdgeDataSource.


        :param destination_pk_column_map: The destination_pk_column_map of this EdgeDataSource.
        :type destination_pk_column_map: Dict[str, object]
        """

        self._destination_pk_column_map = destination_pk_column_map

    @property
    def property_mapping(self) -> Dict[str, object]:
        """Gets the property_mapping of this EdgeDataSource.


        :return: The property_mapping of this EdgeDataSource.
        :rtype: Dict[str, object]
        """
        return self._property_mapping

    @property_mapping.setter
    def property_mapping(self, property_mapping: Dict[str, object]):
        """Sets the property_mapping of this EdgeDataSource.


        :param property_mapping: The property_mapping of this EdgeDataSource.
        :type property_mapping: Dict[str, object]
        """

        self._property_mapping = property_mapping
