from datetime import date, datetime  # noqa: F401

from typing import List, Dict  # noqa: F401

from gs_flex_coordinator.models.base_model import Model
from gs_flex_coordinator import util


class SchemaMappingLoadingConfigDataSource(Model):
    """NOTE: This class is auto generated by OpenAPI Generator (https://openapi-generator.tech).

    Do not edit the class manually.
    """

    def __init__(self, scheme=None):  # noqa: E501
        """SchemaMappingLoadingConfigDataSource - a model defined in OpenAPI

        :param scheme: The scheme of this SchemaMappingLoadingConfigDataSource.  # noqa: E501
        :type scheme: str
        """
        self.openapi_types = {
            'scheme': str
        }

        self.attribute_map = {
            'scheme': 'scheme'
        }

        self._scheme = scheme

    @classmethod
    def from_dict(cls, dikt) -> 'SchemaMappingLoadingConfigDataSource':
        """Returns the dict as a model

        :param dikt: A dict.
        :type: dict
        :return: The SchemaMapping_loading_config_data_source of this SchemaMappingLoadingConfigDataSource.  # noqa: E501
        :rtype: SchemaMappingLoadingConfigDataSource
        """
        return util.deserialize_model(dikt, cls)

    @property
    def scheme(self) -> str:
        """Gets the scheme of this SchemaMappingLoadingConfigDataSource.


        :return: The scheme of this SchemaMappingLoadingConfigDataSource.
        :rtype: str
        """
        return self._scheme

    @scheme.setter
    def scheme(self, scheme: str):
        """Sets the scheme of this SchemaMappingLoadingConfigDataSource.


        :param scheme: The scheme of this SchemaMappingLoadingConfigDataSource.
        :type scheme: str
        """
        allowed_values = ["file"]  # noqa: E501
        if scheme not in allowed_values:
            raise ValueError(
                "Invalid value for `scheme` ({0}), must be one of {1}"
                .format(scheme, allowed_values)
            )

        self._scheme = scheme
