# BasePropertyMeta


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**property_name** | **str** |  | [optional] 
**property_type** | [**GSDataType**](GSDataType.md) |  | [optional] 

## Example

```python
from interactive_sdk.models.base_property_meta import BasePropertyMeta

# TODO update the JSON string below
json = "{}"
# create an instance of BasePropertyMeta from a JSON string
base_property_meta_instance = BasePropertyMeta.from_json(json)
# print the JSON string representation of the object
print BasePropertyMeta.to_json()

# convert the object into a dict
base_property_meta_dict = base_property_meta_instance.to_dict()
# create an instance of BasePropertyMeta from a dict
base_property_meta_form_dict = base_property_meta.from_dict(base_property_meta_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


