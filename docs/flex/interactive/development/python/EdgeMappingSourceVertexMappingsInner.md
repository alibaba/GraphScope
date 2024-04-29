# EdgeMappingSourceVertexMappingsInner

Mapping column to the primary key of source vertex

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**column** | [**EdgeMappingSourceVertexMappingsInnerColumn**](EdgeMappingSourceVertexMappingsInnerColumn.md) |  | [optional] 
**var_property** | **str** |  | [optional] 

## Example

```python
from interactive_sdk.models.edge_mapping_source_vertex_mappings_inner import EdgeMappingSourceVertexMappingsInner

# TODO update the JSON string below
json = "{}"
# create an instance of EdgeMappingSourceVertexMappingsInner from a JSON string
edge_mapping_source_vertex_mappings_inner_instance = EdgeMappingSourceVertexMappingsInner.from_json(json)
# print the JSON string representation of the object
print EdgeMappingSourceVertexMappingsInner.to_json()

# convert the object into a dict
edge_mapping_source_vertex_mappings_inner_dict = edge_mapping_source_vertex_mappings_inner_instance.to_dict()
# create an instance of EdgeMappingSourceVertexMappingsInner from a dict
edge_mapping_source_vertex_mappings_inner_form_dict = edge_mapping_source_vertex_mappings_inner.from_dict(edge_mapping_source_vertex_mappings_inner_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


