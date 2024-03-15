# EdgeTypeVertexTypePairRelationsInner


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**source_vertex** | **str** |  | [optional] 
**destination_vertex** | **str** |  | [optional] 
**relation** | **str** |  | [optional] 
**x_csr_params** | [**EdgeTypeVertexTypePairRelationsInnerXCsrParams**](EdgeTypeVertexTypePairRelationsInnerXCsrParams.md) |  | [optional] 

## Example

```python
from interactive_sdk.models.edge_type_vertex_type_pair_relations_inner import EdgeTypeVertexTypePairRelationsInner

# TODO update the JSON string below
json = "{}"
# create an instance of EdgeTypeVertexTypePairRelationsInner from a JSON string
edge_type_vertex_type_pair_relations_inner_instance = EdgeTypeVertexTypePairRelationsInner.from_json(json)
# print the JSON string representation of the object
print EdgeTypeVertexTypePairRelationsInner.to_json()

# convert the object into a dict
edge_type_vertex_type_pair_relations_inner_dict = edge_type_vertex_type_pair_relations_inner_instance.to_dict()
# create an instance of EdgeTypeVertexTypePairRelationsInner from a dict
edge_type_vertex_type_pair_relations_inner_form_dict = edge_type_vertex_type_pair_relations_inner.from_dict(edge_type_vertex_type_pair_relations_inner_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


