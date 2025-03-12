# ResourceUsage


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**host** | **str** |  | 
**timestamp** | **str** |  | 
**usage** | **str** |  | 

## Example

```python
from graphscope.flex.rest.models.resource_usage import ResourceUsage

# TODO update the JSON string below
json = "{}"
# create an instance of ResourceUsage from a JSON string
resource_usage_instance = ResourceUsage.from_json(json)
# print the JSON string representation of the object
print(ResourceUsage.to_json())

# convert the object into a dict
resource_usage_dict = resource_usage_instance.to_dict()
# create an instance of ResourceUsage from a dict
resource_usage_from_dict = ResourceUsage.from_dict(resource_usage_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


