from datetime import date, datetime  # noqa: F401

from typing import List, Dict  # noqa: F401

from gs_flex_coordinator.models.base_model import Model
from gs_flex_coordinator import util


class NodeStatus(Model):
    """NOTE: This class is auto generated by OpenAPI Generator (https://openapi-generator.tech).

    Do not edit the class manually.
    """

    def __init__(self, node=None, cpu_usage=None, memory_usage=None, disk_usage=None):  # noqa: E501
        """NodeStatus - a model defined in OpenAPI

        :param node: The node of this NodeStatus.  # noqa: E501
        :type node: str
        :param cpu_usage: The cpu_usage of this NodeStatus.  # noqa: E501
        :type cpu_usage: float
        :param memory_usage: The memory_usage of this NodeStatus.  # noqa: E501
        :type memory_usage: float
        :param disk_usage: The disk_usage of this NodeStatus.  # noqa: E501
        :type disk_usage: float
        """
        self.openapi_types = {
            'node': str,
            'cpu_usage': float,
            'memory_usage': float,
            'disk_usage': float
        }

        self.attribute_map = {
            'node': 'node',
            'cpu_usage': 'cpu_usage',
            'memory_usage': 'memory_usage',
            'disk_usage': 'disk_usage'
        }

        self._node = node
        self._cpu_usage = cpu_usage
        self._memory_usage = memory_usage
        self._disk_usage = disk_usage

    @classmethod
    def from_dict(cls, dikt) -> 'NodeStatus':
        """Returns the dict as a model

        :param dikt: A dict.
        :type: dict
        :return: The NodeStatus of this NodeStatus.  # noqa: E501
        :rtype: NodeStatus
        """
        return util.deserialize_model(dikt, cls)

    @property
    def node(self) -> str:
        """Gets the node of this NodeStatus.


        :return: The node of this NodeStatus.
        :rtype: str
        """
        return self._node

    @node.setter
    def node(self, node: str):
        """Sets the node of this NodeStatus.


        :param node: The node of this NodeStatus.
        :type node: str
        """

        self._node = node

    @property
    def cpu_usage(self) -> float:
        """Gets the cpu_usage of this NodeStatus.


        :return: The cpu_usage of this NodeStatus.
        :rtype: float
        """
        return self._cpu_usage

    @cpu_usage.setter
    def cpu_usage(self, cpu_usage: float):
        """Sets the cpu_usage of this NodeStatus.


        :param cpu_usage: The cpu_usage of this NodeStatus.
        :type cpu_usage: float
        """

        self._cpu_usage = cpu_usage

    @property
    def memory_usage(self) -> float:
        """Gets the memory_usage of this NodeStatus.


        :return: The memory_usage of this NodeStatus.
        :rtype: float
        """
        return self._memory_usage

    @memory_usage.setter
    def memory_usage(self, memory_usage: float):
        """Sets the memory_usage of this NodeStatus.


        :param memory_usage: The memory_usage of this NodeStatus.
        :type memory_usage: float
        """

        self._memory_usage = memory_usage

    @property
    def disk_usage(self) -> float:
        """Gets the disk_usage of this NodeStatus.


        :return: The disk_usage of this NodeStatus.
        :rtype: float
        """
        return self._disk_usage

    @disk_usage.setter
    def disk_usage(self, disk_usage: float):
        """Sets the disk_usage of this NodeStatus.


        :param disk_usage: The disk_usage of this NodeStatus.
        :type disk_usage: float
        """

        self._disk_usage = disk_usage
