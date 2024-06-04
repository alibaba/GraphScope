# GSDataType


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**primitive_type** | **str** |  | 
**string** | [**StringTypeString**](StringTypeString.md) |  | 
**timestamp** | **str** |  | 

## Example

```python
from interactive_sdk.openapi.models.gs_data_type import GSDataType

# TODO update the JSON string below
json = "{}"
# create an instance of GSDataType from a JSON string
gs_data_type_instance = GSDataType.from_json(json)
# print the JSON string representation of the object
print GSDataType.to_json()

# convert the object into a dict
gs_data_type_dict = gs_data_type_instance.to_dict()
# create an instance of GSDataType from a dict
gs_data_type_form_dict = gs_data_type.from_dict(gs_data_type_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


