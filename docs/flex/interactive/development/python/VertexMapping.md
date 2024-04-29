# VertexMapping


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**type_name** | **str** |  | [optional] 
**inputs** | **List[str]** |  | [optional] 
**column_mappings** | [**List[ColumnMapping]**](ColumnMapping.md) |  | [optional] 

## Example

```python
from interactive_sdk.models.vertex_mapping import VertexMapping

# TODO update the JSON string below
json = "{}"
# create an instance of VertexMapping from a JSON string
vertex_mapping_instance = VertexMapping.from_json(json)
# print the JSON string representation of the object
print VertexMapping.to_json()

# convert the object into a dict
vertex_mapping_dict = vertex_mapping_instance.to_dict()
# create an instance of VertexMapping from a dict
vertex_mapping_form_dict = vertex_mapping.from_dict(vertex_mapping_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


