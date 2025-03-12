# GetResourceUsageResponse


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**cpu_usage** | [**List[ResourceUsage]**](ResourceUsage.md) |  | 
**memory_usage** | [**List[ResourceUsage]**](ResourceUsage.md) |  | 

## Example

```python
from graphscope.flex.rest.models.get_resource_usage_response import GetResourceUsageResponse

# TODO update the JSON string below
json = "{}"
# create an instance of GetResourceUsageResponse from a JSON string
get_resource_usage_response_instance = GetResourceUsageResponse.from_json(json)
# print the JSON string representation of the object
print(GetResourceUsageResponse.to_json())

# convert the object into a dict
get_resource_usage_response_dict = get_resource_usage_response_instance.to_dict()
# create an instance of GetResourceUsageResponse from a dict
get_resource_usage_response_from_dict = GetResourceUsageResponse.from_dict(get_resource_usage_response_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


