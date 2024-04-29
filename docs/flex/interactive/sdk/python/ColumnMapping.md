# ColumnMapping


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**column** | [**EdgeMappingSourceVertexMappingsInnerColumn**](EdgeMappingSourceVertexMappingsInnerColumn.md) |  | [optional] 
**var_property** | **str** | must align with the schema | [optional] 

## Example

```python
from interactive_sdk.models.column_mapping import ColumnMapping

# TODO update the JSON string below
json = "{}"
# create an instance of ColumnMapping from a JSON string
column_mapping_instance = ColumnMapping.from_json(json)
# print the JSON string representation of the object
print ColumnMapping.to_json()

# convert the object into a dict
column_mapping_dict = column_mapping_instance.to_dict()
# create an instance of ColumnMapping from a dict
column_mapping_form_dict = column_mapping.from_dict(column_mapping_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


