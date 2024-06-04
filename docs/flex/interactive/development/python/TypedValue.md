# TypedValue


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**type** | [**GSDataType**](GSDataType.md) |  | 
**value** | **object** |  | 

## Example

```python
from interactive_sdk.openapi.models.typed_value import TypedValue

# TODO update the JSON string below
json = "{}"
# create an instance of TypedValue from a JSON string
typed_value_instance = TypedValue.from_json(json)
# print the JSON string representation of the object
print TypedValue.to_json()

# convert the object into a dict
typed_value_dict = typed_value_instance.to_dict()
# create an instance of TypedValue from a dict
typed_value_form_dict = typed_value.from_dict(typed_value_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


