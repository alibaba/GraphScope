# ColumnMappingColumn


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**index** | **int** |  | [optional] 
**name** | **str** |  | [optional] 

## Example

```python
from graphscope.flex.rest.models.column_mapping_column import ColumnMappingColumn

# TODO update the JSON string below
json = "{}"
# create an instance of ColumnMappingColumn from a JSON string
column_mapping_column_instance = ColumnMappingColumn.from_json(json)
# print the JSON string representation of the object
print(ColumnMappingColumn.to_json())

# convert the object into a dict
column_mapping_column_dict = column_mapping_column_instance.to_dict()
# create an instance of ColumnMappingColumn from a dict
column_mapping_column_from_dict = ColumnMappingColumn.from_dict(column_mapping_column_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


