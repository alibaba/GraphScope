# ServiceStatusSdkEndpoints


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**cypher** | **str** |  | [optional] 
**gremlin** | **str** |  | [optional] 
**hqps** | **str** |  | [optional] 
**grpc** | **str** |  | [optional] 

## Example

```python
from graphscope.flex.rest.models.service_status_sdk_endpoints import ServiceStatusSdkEndpoints

# TODO update the JSON string below
json = "{}"
# create an instance of ServiceStatusSdkEndpoints from a JSON string
service_status_sdk_endpoints_instance = ServiceStatusSdkEndpoints.from_json(json)
# print the JSON string representation of the object
print(ServiceStatusSdkEndpoints.to_json())

# convert the object into a dict
service_status_sdk_endpoints_dict = service_status_sdk_endpoints_instance.to_dict()
# create an instance of ServiceStatusSdkEndpoints from a dict
service_status_sdk_endpoints_from_dict = ServiceStatusSdkEndpoints.from_dict(service_status_sdk_endpoints_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


