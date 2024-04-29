# GetPropertyMeta


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**property_name** | **str** |  | [optional] 
**property_type** | [**GSDataType**](GSDataType.md) |  | [optional] 
**property_id** | **int** |  | [optional] 

## Example

```python
from interactive_sdk.models.get_property_meta import GetPropertyMeta

# TODO update the JSON string below
json = "{}"
# create an instance of GetPropertyMeta from a JSON string
get_property_meta_instance = GetPropertyMeta.from_json(json)
# print the JSON string representation of the object
print GetPropertyMeta.to_json()

# convert the object into a dict
get_property_meta_dict = get_property_meta_instance.to_dict()
# create an instance of GetPropertyMeta from a dict
get_property_meta_form_dict = get_property_meta.from_dict(get_property_meta_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


