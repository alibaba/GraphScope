# StringTypeString


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**long_text** | **str** |  | 
**char** | [**FixedCharChar**](FixedCharChar.md) |  | 
**var_char** | [**VarCharVarChar**](VarCharVarChar.md) |  | 

## Example

```python
from interactive_sdk.models.string_type_string import StringTypeString

# TODO update the JSON string below
json = "{}"
# create an instance of StringTypeString from a JSON string
string_type_string_instance = StringTypeString.from_json(json)
# print the JSON string representation of the object
print StringTypeString.to_json()

# convert the object into a dict
string_type_string_dict = string_type_string_instance.to_dict()
# create an instance of StringTypeString from a dict
string_type_string_form_dict = string_type_string.from_dict(string_type_string_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


