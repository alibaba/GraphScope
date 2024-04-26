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

from interactive_sdk.models.edge_mapping import EdgeMapping
from interactive_sdk.models.schema_mapping_loading_config import \
    SchemaMappingLoadingConfig
from interactive_sdk.models.vertex_mapping import VertexMapping

try:
    from typing import Self
except ImportError:
    from typing_extensions import Self


class SchemaMapping(BaseModel):
    """
    SchemaMapping
    """  # noqa: E501

    graph: Optional[StrictStr] = None
    loading_config: Optional[SchemaMappingLoadingConfig] = None
    vertex_mappings: Optional[List[VertexMapping]] = None
    edge_mappings: Optional[List[EdgeMapping]] = None
    __properties: ClassVar[List[str]] = [
        "graph",
        "loading_config",
        "vertex_mappings",
        "edge_mappings",
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
        """Create an instance of SchemaMapping from a JSON string"""
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
        # override the default output from pydantic by calling `to_dict()` of loading_config
        if self.loading_config:
            _dict["loading_config"] = self.loading_config.to_dict()
        # override the default output from pydantic by calling `to_dict()` of each item in vertex_mappings (list)
        _items = []
        if self.vertex_mappings:
            for _item in self.vertex_mappings:
                if _item:
                    _items.append(_item.to_dict())
            _dict["vertex_mappings"] = _items
        # override the default output from pydantic by calling `to_dict()` of each item in edge_mappings (list)
        _items = []
        if self.edge_mappings:
            for _item in self.edge_mappings:
                if _item:
                    _items.append(_item.to_dict())
            _dict["edge_mappings"] = _items
        return _dict

    @classmethod
    def from_dict(cls, obj: Dict) -> Self:
        """Create an instance of SchemaMapping from a dict"""
        if obj is None:
            return None

        if not isinstance(obj, dict):
            return cls.model_validate(obj)

        _obj = cls.model_validate(
            {
                "graph": obj.get("graph"),
                "loading_config": (
                    SchemaMappingLoadingConfig.from_dict(obj.get("loading_config"))
                    if obj.get("loading_config") is not None
                    else None
                ),
                "vertex_mappings": (
                    [
                        VertexMapping.from_dict(_item)
                        for _item in obj.get("vertex_mappings")
                    ]
                    if obj.get("vertex_mappings") is not None
                    else None
                ),
                "edge_mappings": (
                    [EdgeMapping.from_dict(_item) for _item in obj.get("edge_mappings")]
                    if obj.get("edge_mappings") is not None
                    else None
                ),
            }
        )
        return _obj
