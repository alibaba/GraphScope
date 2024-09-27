from datetime import date, datetime  # noqa: F401

from typing import List, Dict  # noqa: F401

from gscoordinator.flex.models.base_model import Model
from gscoordinator.flex import util


class TimeStampType(Model):
    """NOTE: This class is auto generated by OpenAPI Generator (https://openapi-generator.tech).

    Do not edit the class manually.
    """

    def __init__(self, timestamp=None):  # noqa: E501
        """TimeStampType - a model defined in OpenAPI

        :param timestamp: The timestamp of this TimeStampType.  # noqa: E501
        :type timestamp: str
        """
        self.openapi_types = {
            'timestamp': str
        }

        self.attribute_map = {
            'timestamp': 'timestamp'
        }

        self._timestamp = timestamp

    @classmethod
    def from_dict(cls, dikt) -> 'TimeStampType':
        """Returns the dict as a model

        :param dikt: A dict.
        :type: dict
        :return: The TimeStampType of this TimeStampType.  # noqa: E501
        :rtype: TimeStampType
        """
        return util.deserialize_model(dikt, cls)

    @property
    def timestamp(self) -> str:
        """Gets the timestamp of this TimeStampType.


        :return: The timestamp of this TimeStampType.
        :rtype: str
        """
        return self._timestamp

    @timestamp.setter
    def timestamp(self, timestamp: str):
        """Sets the timestamp of this TimeStampType.


        :param timestamp: The timestamp of this TimeStampType.
        :type timestamp: str
        """
        if timestamp is None:
            raise ValueError("Invalid value for `timestamp`, must not be `None`")  # noqa: E501

        self._timestamp = timestamp