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

from pydantic import BaseModel, StrictStr, field_validator

from interactive_sdk.models.base_edge_type_vertex_type_pair_relations_inner_x_csr_params import \
    BaseEdgeTypeVertexTypePairRelationsInnerXCsrParams

try:
    from typing import Self
except ImportError:
    from typing_extensions import Self


class BaseEdgeTypeVertexTypePairRelationsInner(BaseModel):
    """
    BaseEdgeTypeVertexTypePairRelationsInner
    """  # noqa: E501

    source_vertex: Optional[StrictStr] = None
    destination_vertex: Optional[StrictStr] = None
    relation: Optional[StrictStr] = None
    x_csr_params: Optional[BaseEdgeTypeVertexTypePairRelationsInnerXCsrParams] = None
    __properties: ClassVar[List[str]] = [
        "source_vertex",
        "destination_vertex",
        "relation",
        "x_csr_params",
    ]

    @field_validator("relation")
    def relation_validate_enum(cls, value):
        """Validates the enum"""
        if value is None:
            return value

        if value not in ("MANY_TO_MANY", "ONE_TO_MANY", "MANY_TO_ONE", "ONE_TO_ONE"):
            raise ValueError(
                "must be one of enum values ('MANY_TO_MANY', 'ONE_TO_MANY', 'MANY_TO_ONE', 'ONE_TO_ONE')"
            )
        return value

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
        """Create an instance of BaseEdgeTypeVertexTypePairRelationsInner from a JSON string"""
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
        return _dict

    @classmethod
    def from_dict(cls, obj: Dict) -> Self:
        """Create an instance of BaseEdgeTypeVertexTypePairRelationsInner from a dict"""
        if obj is None:
            return None

        if not isinstance(obj, dict):
            return cls.model_validate(obj)

        _obj = cls.model_validate(
            {
                "source_vertex": obj.get("source_vertex"),
                "destination_vertex": obj.get("destination_vertex"),
                "relation": obj.get("relation"),
                "x_csr_params": (
                    BaseEdgeTypeVertexTypePairRelationsInnerXCsrParams.from_dict(
                        obj.get("x_csr_params")
                    )
                    if obj.get("x_csr_params") is not None
                    else None
                ),
            }
        )
        return _obj
