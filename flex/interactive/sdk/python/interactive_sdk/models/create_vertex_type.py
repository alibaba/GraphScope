# coding: utf-8

"""
    GraphScope Interactive API v0.0.3

    This is the definition of GraphScope Interactive API, including   - AdminService API   - Vertex/Edge API   - QueryService   AdminService API (with tag AdminService) defines the API for GraphManagement, ProcedureManagement and Service Management.  Vertex/Edge API (with tag GraphService) defines the API for Vertex/Edge management, including creation/updating/delete/retrive.  QueryService API (with tag QueryService) defines the API for procedure_call, Ahodc query. 

    The version of the OpenAPI document: 1.0.0
    Contact: graphscope@alibaba-inc.com
    Generated by OpenAPI Generator (https://openapi-generator.tech)

    Do not edit the class manually.
"""  # noqa: E501


from __future__ import annotations

import json
import pprint
import re  # noqa: F401
from typing import Any, ClassVar, Dict, List, Optional

from pydantic import BaseModel, StrictStr

from interactive_sdk.models.base_vertex_type_x_csr_params import \
    BaseVertexTypeXCsrParams
from interactive_sdk.models.create_property_meta import CreatePropertyMeta

try:
    from typing import Self
except ImportError:
    from typing_extensions import Self


class CreateVertexType(BaseModel):
    """
    CreateVertexType
    """  # noqa: E501

    type_name: Optional[StrictStr] = None
    primary_keys: Optional[List[StrictStr]] = None
    x_csr_params: Optional[BaseVertexTypeXCsrParams] = None
    properties: Optional[List[CreatePropertyMeta]] = None
    __properties: ClassVar[List[str]] = [
        "type_name",
        "primary_keys",
        "x_csr_params",
        "properties",
    ]

    model_config = {
        "populate_by_name": True,
        "validate_assignment": True,
        "protected_namespaces": (),
    }

    def to_str(self) -> str:
        """Returns the string representation of the model using alias"""
        return pprint.pformat(self.model_dump(by_alias=True))

    def to_json(self) -> str:
        """Returns the JSON representation of the model using alias"""
        # TODO: pydantic v2: use .model_dump_json(by_alias=True, exclude_unset=True) instead
        return json.dumps(self.to_dict())

    @classmethod
    def from_json(cls, json_str: str) -> Self:
        """Create an instance of CreateVertexType from a JSON string"""
        return cls.from_dict(json.loads(json_str))

    def to_dict(self) -> Dict[str, Any]:
        """Return the dictionary representation of the model using alias.

        This has the following differences from calling pydantic's
        `self.model_dump(by_alias=True)`:

        * `None` is only added to the output dict for nullable fields that
          were set at model initialization. Other fields with value `None`
          are ignored.
        """
        _dict = self.model_dump(
            by_alias=True,
            exclude={},
            exclude_none=True,
        )
        # override the default output from pydantic by calling `to_dict()` of x_csr_params
        if self.x_csr_params:
            _dict["x_csr_params"] = self.x_csr_params.to_dict()
        # override the default output from pydantic by calling `to_dict()` of each item in properties (list)
        _items = []
        if self.properties:
            for _item in self.properties:
                if _item:
                    _items.append(_item.to_dict())
            _dict["properties"] = _items
        return _dict

    @classmethod
    def from_dict(cls, obj: Dict) -> Self:
        """Create an instance of CreateVertexType from a dict"""
        if obj is None:
            return None

        if not isinstance(obj, dict):
            return cls.model_validate(obj)

        _obj = cls.model_validate(
            {
                "type_name": obj.get("type_name"),
                "primary_keys": obj.get("primary_keys"),
                "x_csr_params": (
                    BaseVertexTypeXCsrParams.from_dict(obj.get("x_csr_params"))
                    if obj.get("x_csr_params") is not None
                    else None
                ),
                "properties": (
                    [
                        CreatePropertyMeta.from_dict(_item)
                        for _item in obj.get("properties")
                    ]
                    if obj.get("properties") is not None
                    else None
                ),
            }
        )
        return _obj
