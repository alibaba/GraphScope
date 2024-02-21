# coding: utf-8

"""
    GraphScope FLEX HTTP SERVICE API

    This is a specification for GraphScope FLEX HTTP service based on the OpenAPI 3.0 specification. You can find out more details about specification at [doc](https://swagger.io/specification/v3/).  Some useful links: - [GraphScope Repository](https://github.com/alibaba/GraphScope) - [The Source API definition for GraphScope Interactive](https://github.com/GraphScope/portal/tree/main/httpservice)

    The version of the OpenAPI document: 0.9.1
    Contact: graphscope@alibaba-inc.com
    Generated by OpenAPI Generator (https://openapi-generator.tech)

    Do not edit the class manually.
"""  # noqa: E501


from __future__ import annotations
import pprint
import re  # noqa: F401
import json

from pydantic import BaseModel
from typing import Any, ClassVar, Dict, List, Optional
from graphscope.flex.rest.models.groot_edge_type import GrootEdgeType
from graphscope.flex.rest.models.groot_vertex_type import GrootVertexType
from typing import Optional, Set
from typing_extensions import Self

class GrootSchema(BaseModel):
    """
    GrootSchema
    """ # noqa: E501
    vertices: Optional[List[GrootVertexType]] = None
    edges: Optional[List[GrootEdgeType]] = None
    __properties: ClassVar[List[str]] = ["vertices", "edges"]

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
    def from_json(cls, json_str: str) -> Optional[Self]:
        """Create an instance of GrootSchema from a JSON string"""
        return cls.from_dict(json.loads(json_str))

    def to_dict(self) -> Dict[str, Any]:
        """Return the dictionary representation of the model using alias.

        This has the following differences from calling pydantic's
        `self.model_dump(by_alias=True)`:

        * `None` is only added to the output dict for nullable fields that
          were set at model initialization. Other fields with value `None`
          are ignored.
        """
        excluded_fields: Set[str] = set([
        ])

        _dict = self.model_dump(
            by_alias=True,
            exclude=excluded_fields,
            exclude_none=True,
        )
        # override the default output from pydantic by calling `to_dict()` of each item in vertices (list)
        _items = []
        if self.vertices:
            for _item in self.vertices:
                if _item:
                    _items.append(_item.to_dict())
            _dict['vertices'] = _items
        # override the default output from pydantic by calling `to_dict()` of each item in edges (list)
        _items = []
        if self.edges:
            for _item in self.edges:
                if _item:
                    _items.append(_item.to_dict())
            _dict['edges'] = _items
        return _dict

    @classmethod
    def from_dict(cls, obj: Optional[Dict[str, Any]]) -> Optional[Self]:
        """Create an instance of GrootSchema from a dict"""
        if obj is None:
            return None

        if not isinstance(obj, dict):
            return cls.model_validate(obj)

        _obj = cls.model_validate({
            "vertices": [GrootVertexType.from_dict(_item) for _item in obj["vertices"]] if obj.get("vertices") is not None else None,
            "edges": [GrootEdgeType.from_dict(_item) for _item in obj["edges"]] if obj.get("edges") is not None else None
        })
        return _obj


