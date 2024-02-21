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

from pydantic import BaseModel, Field, StrictBool, StrictStr, field_validator
from typing import Any, ClassVar, Dict, List, Optional
from graphscope.flex.rest.models.groot_graph_gremlin_interface import GrootGraphGremlinInterface
from graphscope.flex.rest.models.groot_schema import GrootSchema
from typing import Optional, Set
from typing_extensions import Self

class GrootGraph(BaseModel):
    """
    GrootGraph
    """ # noqa: E501
    name: Optional[StrictStr] = None
    type: Optional[StrictStr] = None
    directed: Optional[StrictBool] = None
    creation_time: Optional[StrictStr] = None
    var_schema: Optional[GrootSchema] = Field(default=None, alias="schema")
    gremlin_interface: Optional[GrootGraphGremlinInterface] = None
    __properties: ClassVar[List[str]] = ["name", "type", "directed", "creation_time", "schema", "gremlin_interface"]

    @field_validator('type')
    def type_validate_enum(cls, value):
        """Validates the enum"""
        if value is None:
            return value

        if value not in set(['GrootGraph']):
            raise ValueError("must be one of enum values ('GrootGraph')")
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
    def from_json(cls, json_str: str) -> Optional[Self]:
        """Create an instance of GrootGraph from a JSON string"""
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
        # override the default output from pydantic by calling `to_dict()` of var_schema
        if self.var_schema:
            _dict['schema'] = self.var_schema.to_dict()
        # override the default output from pydantic by calling `to_dict()` of gremlin_interface
        if self.gremlin_interface:
            _dict['gremlin_interface'] = self.gremlin_interface.to_dict()
        return _dict

    @classmethod
    def from_dict(cls, obj: Optional[Dict[str, Any]]) -> Optional[Self]:
        """Create an instance of GrootGraph from a dict"""
        if obj is None:
            return None

        if not isinstance(obj, dict):
            return cls.model_validate(obj)

        _obj = cls.model_validate({
            "name": obj.get("name"),
            "type": obj.get("type"),
            "directed": obj.get("directed"),
            "creation_time": obj.get("creation_time"),
            "schema": GrootSchema.from_dict(obj["schema"]) if obj.get("schema") is not None else None,
            "gremlin_interface": GrootGraphGremlinInterface.from_dict(obj["gremlin_interface"]) if obj.get("gremlin_interface") is not None else None
        })
        return _obj


