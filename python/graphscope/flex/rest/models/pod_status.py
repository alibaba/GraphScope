# coding: utf-8

"""
    GraphScope FLEX HTTP SERVICE API

    This is a specification for GraphScope FLEX HTTP service based on the OpenAPI 3.0 specification. You can find out more details about specification at [doc](https://swagger.io/specification/v3/).  Some useful links: - [GraphScope Repository](https://github.com/alibaba/GraphScope) - [The Source API definition for GraphScope Interactive](https://github.com/GraphScope/portal/tree/main/httpservice)

    The version of the OpenAPI document: 1.0.0
    Contact: graphscope@alibaba-inc.com
    Generated by OpenAPI Generator (https://openapi-generator.tech)

    Do not edit the class manually.
"""  # noqa: E501


from __future__ import annotations
import pprint
import re  # noqa: F401
import json

from pydantic import BaseModel, StrictInt, StrictStr
from typing import Any, ClassVar, Dict, List, Optional
from typing import Optional, Set
from typing_extensions import Self

class PodStatus(BaseModel):
    """
    PodStatus
    """ # noqa: E501
    name: StrictStr
    image: List[StrictStr]
    labels: Dict[str, Any]
    node: StrictStr
    status: StrictStr
    restart_count: StrictInt
    cpu_usage: StrictInt
    memory_usage: StrictInt
    timestamp: Optional[StrictStr] = None
    creation_time: StrictStr
    component_belong_to: Optional[StrictStr] = None
    __properties: ClassVar[List[str]] = ["name", "image", "labels", "node", "status", "restart_count", "cpu_usage", "memory_usage", "timestamp", "creation_time", "component_belong_to"]

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
        """Create an instance of PodStatus from a JSON string"""
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
        return _dict

    @classmethod
    def from_dict(cls, obj: Optional[Dict[str, Any]]) -> Optional[Self]:
        """Create an instance of PodStatus from a dict"""
        if obj is None:
            return None

        if not isinstance(obj, dict):
            return cls.model_validate(obj)

        _obj = cls.model_validate({
            "name": obj.get("name"),
            "image": obj.get("image"),
            "labels": obj.get("labels"),
            "node": obj.get("node"),
            "status": obj.get("status"),
            "restart_count": obj.get("restart_count"),
            "cpu_usage": obj.get("cpu_usage"),
            "memory_usage": obj.get("memory_usage"),
            "timestamp": obj.get("timestamp"),
            "creation_time": obj.get("creation_time"),
            "component_belong_to": obj.get("component_belong_to")
        })
        return _obj


