# GSDataType


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**primitive_type** | **str** |  | 
**string** | [**StringTypeString**](StringTypeString.md) |  | 
**temporal** | [**TemporalTypeTemporal**](TemporalTypeTemporal.md) |  | 

## Example

```python
from graphscope.flex.rest.models.gs_data_type import GSDataType

# TODO update the JSON string below
json = "{}"
# create an instance of GSDataType from a JSON string
gs_data_type_instance = GSDataType.from_json(json)
# print the JSON string representation of the object
print(GSDataType.to_json())

# convert the object into a dict
gs_data_type_dict = gs_data_type_instance.to_dict()
# create an instance of GSDataType from a dict
gs_data_type_from_dict = GSDataType.from_dict(gs_data_type_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


