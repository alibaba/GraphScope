# GetStorageUsageResponse


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**storage_usage** | **Dict[str, float]** |  | 

## Example

```python
from graphscope.flex.rest.models.get_storage_usage_response import GetStorageUsageResponse

# TODO update the JSON string below
json = "{}"
# create an instance of GetStorageUsageResponse from a JSON string
get_storage_usage_response_instance = GetStorageUsageResponse.from_json(json)
# print the JSON string representation of the object
print(GetStorageUsageResponse.to_json())

# convert the object into a dict
get_storage_usage_response_dict = get_storage_usage_response_instance.to_dict()
# create an instance of GetStorageUsageResponse from a dict
get_storage_usage_response_from_dict = GetStorageUsageResponse.from_dict(get_storage_usage_response_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


