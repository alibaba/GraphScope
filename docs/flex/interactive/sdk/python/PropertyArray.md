# PropertyArray


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**properties** | [**List[ModelProperty]**](ModelProperty.md) |  | 

## Example

```python
from interactive_sdk.models.property_array import PropertyArray

# TODO update the JSON string below
json = "{}"
# create an instance of PropertyArray from a JSON string
property_array_instance = PropertyArray.from_json(json)
# print the JSON string representation of the object
print PropertyArray.to_json()

# convert the object into a dict
property_array_dict = property_array_instance.to_dict()
# create an instance of PropertyArray from a dict
property_array_form_dict = property_array.from_dict(property_array_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


