# Element


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**type** | [**GSDataType**](GSDataType.md) |  | 
**value** | **object** |  | 

## Example

```python
from interactive_sdk.models.element import Element

# TODO update the JSON string below
json = "{}"
# create an instance of Element from a JSON string
element_instance = Element.from_json(json)
# print the JSON string representation of the object
print Element.to_json()

# convert the object into a dict
element_dict = element_instance.to_dict()
# create an instance of Element from a dict
element_form_dict = element.from_dict(element_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


