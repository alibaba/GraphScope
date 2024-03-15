# PropertyMeta


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**property_id** | **int** |  | [optional] 
**property_name** | **str** |  | [optional] 
**property_type** | [**PropertyMetaPropertyType**](PropertyMetaPropertyType.md) |  | [optional] 

## Example

```python
from interactive_sdk.models.property_meta import PropertyMeta

# TODO update the JSON string below
json = "{}"
# create an instance of PropertyMeta from a JSON string
property_meta_instance = PropertyMeta.from_json(json)
# print the JSON string representation of the object
print PropertyMeta.to_json()

# convert the object into a dict
property_meta_dict = property_meta_instance.to_dict()
# create an instance of PropertyMeta from a dict
property_meta_form_dict = property_meta.from_dict(property_meta_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


