from datetime import date, datetime  # noqa: F401

from typing import List, Dict  # noqa: F401

from gscoordinator.flex.models.base_model import Model
from gscoordinator.flex import util


class GetAlertReceiverResponse(Model):
    """NOTE: This class is auto generated by OpenAPI Generator (https://openapi-generator.tech).

    Do not edit the class manually.
    """

    def __init__(self, type=None, webhook_url=None, at_user_ids=None, is_at_all=None, enable=None, id=None, message=None):  # noqa: E501
        """GetAlertReceiverResponse - a model defined in OpenAPI

        :param type: The type of this GetAlertReceiverResponse.  # noqa: E501
        :type type: str
        :param webhook_url: The webhook_url of this GetAlertReceiverResponse.  # noqa: E501
        :type webhook_url: str
        :param at_user_ids: The at_user_ids of this GetAlertReceiverResponse.  # noqa: E501
        :type at_user_ids: List[str]
        :param is_at_all: The is_at_all of this GetAlertReceiverResponse.  # noqa: E501
        :type is_at_all: bool
        :param enable: The enable of this GetAlertReceiverResponse.  # noqa: E501
        :type enable: bool
        :param id: The id of this GetAlertReceiverResponse.  # noqa: E501
        :type id: str
        :param message: The message of this GetAlertReceiverResponse.  # noqa: E501
        :type message: str
        """
        self.openapi_types = {
            'type': str,
            'webhook_url': str,
            'at_user_ids': List[str],
            'is_at_all': bool,
            'enable': bool,
            'id': str,
            'message': str
        }

        self.attribute_map = {
            'type': 'type',
            'webhook_url': 'webhook_url',
            'at_user_ids': 'at_user_ids',
            'is_at_all': 'is_at_all',
            'enable': 'enable',
            'id': 'id',
            'message': 'message'
        }

        self._type = type
        self._webhook_url = webhook_url
        self._at_user_ids = at_user_ids
        self._is_at_all = is_at_all
        self._enable = enable
        self._id = id
        self._message = message

    @classmethod
    def from_dict(cls, dikt) -> 'GetAlertReceiverResponse':
        """Returns the dict as a model

        :param dikt: A dict.
        :type: dict
        :return: The GetAlertReceiverResponse of this GetAlertReceiverResponse.  # noqa: E501
        :rtype: GetAlertReceiverResponse
        """
        return util.deserialize_model(dikt, cls)

    @property
    def type(self) -> str:
        """Gets the type of this GetAlertReceiverResponse.


        :return: The type of this GetAlertReceiverResponse.
        :rtype: str
        """
        return self._type

    @type.setter
    def type(self, type: str):
        """Sets the type of this GetAlertReceiverResponse.


        :param type: The type of this GetAlertReceiverResponse.
        :type type: str
        """
        allowed_values = ["webhook"]  # noqa: E501
        if type not in allowed_values:
            raise ValueError(
                "Invalid value for `type` ({0}), must be one of {1}"
                .format(type, allowed_values)
            )

        self._type = type

    @property
    def webhook_url(self) -> str:
        """Gets the webhook_url of this GetAlertReceiverResponse.


        :return: The webhook_url of this GetAlertReceiverResponse.
        :rtype: str
        """
        return self._webhook_url

    @webhook_url.setter
    def webhook_url(self, webhook_url: str):
        """Sets the webhook_url of this GetAlertReceiverResponse.


        :param webhook_url: The webhook_url of this GetAlertReceiverResponse.
        :type webhook_url: str
        """
        if webhook_url is None:
            raise ValueError("Invalid value for `webhook_url`, must not be `None`")  # noqa: E501

        self._webhook_url = webhook_url

    @property
    def at_user_ids(self) -> List[str]:
        """Gets the at_user_ids of this GetAlertReceiverResponse.


        :return: The at_user_ids of this GetAlertReceiverResponse.
        :rtype: List[str]
        """
        return self._at_user_ids

    @at_user_ids.setter
    def at_user_ids(self, at_user_ids: List[str]):
        """Sets the at_user_ids of this GetAlertReceiverResponse.


        :param at_user_ids: The at_user_ids of this GetAlertReceiverResponse.
        :type at_user_ids: List[str]
        """
        if at_user_ids is None:
            raise ValueError("Invalid value for `at_user_ids`, must not be `None`")  # noqa: E501

        self._at_user_ids = at_user_ids

    @property
    def is_at_all(self) -> bool:
        """Gets the is_at_all of this GetAlertReceiverResponse.


        :return: The is_at_all of this GetAlertReceiverResponse.
        :rtype: bool
        """
        return self._is_at_all

    @is_at_all.setter
    def is_at_all(self, is_at_all: bool):
        """Sets the is_at_all of this GetAlertReceiverResponse.


        :param is_at_all: The is_at_all of this GetAlertReceiverResponse.
        :type is_at_all: bool
        """
        if is_at_all is None:
            raise ValueError("Invalid value for `is_at_all`, must not be `None`")  # noqa: E501

        self._is_at_all = is_at_all

    @property
    def enable(self) -> bool:
        """Gets the enable of this GetAlertReceiverResponse.


        :return: The enable of this GetAlertReceiverResponse.
        :rtype: bool
        """
        return self._enable

    @enable.setter
    def enable(self, enable: bool):
        """Sets the enable of this GetAlertReceiverResponse.


        :param enable: The enable of this GetAlertReceiverResponse.
        :type enable: bool
        """
        if enable is None:
            raise ValueError("Invalid value for `enable`, must not be `None`")  # noqa: E501

        self._enable = enable

    @property
    def id(self) -> str:
        """Gets the id of this GetAlertReceiverResponse.


        :return: The id of this GetAlertReceiverResponse.
        :rtype: str
        """
        return self._id

    @id.setter
    def id(self, id: str):
        """Sets the id of this GetAlertReceiverResponse.


        :param id: The id of this GetAlertReceiverResponse.
        :type id: str
        """
        if id is None:
            raise ValueError("Invalid value for `id`, must not be `None`")  # noqa: E501

        self._id = id

    @property
    def message(self) -> str:
        """Gets the message of this GetAlertReceiverResponse.

        Error message generated in server side  # noqa: E501

        :return: The message of this GetAlertReceiverResponse.
        :rtype: str
        """
        return self._message

    @message.setter
    def message(self, message: str):
        """Sets the message of this GetAlertReceiverResponse.

        Error message generated in server side  # noqa: E501

        :param message: The message of this GetAlertReceiverResponse.
        :type message: str
        """
        if message is None:
            raise ValueError("Invalid value for `message`, must not be `None`")  # noqa: E501

        self._message = message