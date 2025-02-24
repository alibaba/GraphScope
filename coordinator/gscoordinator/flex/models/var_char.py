from datetime import date, datetime  # noqa: F401

from typing import List, Dict  # noqa: F401

from gscoordinator.flex.models.base_model import Model
from gscoordinator.flex.models.var_char_var_char import VarCharVarChar
from gscoordinator.flex import util

from gscoordinator.flex.models.var_char_var_char import VarCharVarChar  # noqa: E501

class VarChar(Model):
    """NOTE: This class is auto generated by OpenAPI Generator (https://openapi-generator.tech).

    Do not edit the class manually.
    """

    def __init__(self, var_char=None):  # noqa: E501
        """VarChar - a model defined in OpenAPI

        :param var_char: The var_char of this VarChar.  # noqa: E501
        :type var_char: VarCharVarChar
        """
        self.openapi_types = {
            'var_char': VarCharVarChar
        }

        self.attribute_map = {
            'var_char': 'var_char'
        }

        self._var_char = var_char

    @classmethod
    def from_dict(cls, dikt) -> 'VarChar':
        """Returns the dict as a model

        :param dikt: A dict.
        :type: dict
        :return: The VarChar of this VarChar.  # noqa: E501
        :rtype: VarChar
        """
        return util.deserialize_model(dikt, cls)

    @property
    def var_char(self) -> VarCharVarChar:
        """Gets the var_char of this VarChar.


        :return: The var_char of this VarChar.
        :rtype: VarCharVarChar
        """
        return self._var_char

    @var_char.setter
    def var_char(self, var_char: VarCharVarChar):
        """Sets the var_char of this VarChar.


        :param var_char: The var_char of this VarChar.
        :type var_char: VarCharVarChar
        """
        if var_char is None:
            raise ValueError("Invalid value for `var_char`, must not be `None`")  # noqa: E501

        self._var_char = var_char
