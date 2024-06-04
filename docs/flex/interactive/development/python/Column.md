# Column


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**type** | [**GSDataType**](GSDataType.md) |  | 
**value** | [**Element**](Element.md) |  | 
**values** | [**List[Element]**](Element.md) |  | [optional] 
**key** | [**TypedValue**](TypedValue.md) |  | 

## Example

```python
from interactive_sdk.openapi.models.column import Column

# TODO update the JSON string below
json = "{}"
# create an instance of Column from a JSON string
column_instance = Column.from_json(json)
# print the JSON string representation of the object
print Column.to_json()

# convert the object into a dict
column_dict = column_instance.to_dict()
# create an instance of Column from a dict
column_form_dict = column.from_dict(column_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


