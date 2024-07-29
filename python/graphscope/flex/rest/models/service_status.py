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

from pydantic import BaseModel, StrictStr, field_validator
from typing import Any, ClassVar, Dict, List, Optional
from graphscope.flex.rest.models.service_status_sdk_endpoints import ServiceStatusSdkEndpoints
from typing import Optional, Set
from typing_extensions import Self

class ServiceStatus(BaseModel):
    """
    ServiceStatus
    """ # noqa: E501
    graph_id: StrictStr
    status: StrictStr
    sdk_endpoints: Optional[ServiceStatusSdkEndpoints] = None
    start_time: Optional[StrictStr] = None
    __properties: ClassVar[List[str]] = ["graph_id", "status", "sdk_endpoints", "start_time"]

    @field_validator('status')
    def status_validate_enum(cls, value):
        """Validates the enum"""
        if value not in set(['Running', 'Stopped']):
            raise ValueError("must be one of enum values ('Running', 'Stopped')")
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
        """Create an instance of ServiceStatus from a JSON string"""
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
        # override the default output from pydantic by calling `to_dict()` of sdk_endpoints
        if self.sdk_endpoints:
            _dict['sdk_endpoints'] = self.sdk_endpoints.to_dict()
        return _dict

    @classmethod
    def from_dict(cls, obj: Optional[Dict[str, Any]]) -> Optional[Self]:
        """Create an instance of ServiceStatus from a dict"""
        if obj is None:
            return None

        if not isinstance(obj, dict):
            return cls.model_validate(obj)

        _obj = cls.model_validate({
            "graph_id": obj.get("graph_id"),
            "status": obj.get("status"),
            "sdk_endpoints": ServiceStatusSdkEndpoints.from_dict(obj["sdk_endpoints"]) if obj.get("sdk_endpoints") is not None else None,
            "start_time": obj.get("start_time")
        })
        return _obj


