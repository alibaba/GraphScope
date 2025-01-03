# BaseEdgeTypeVertexTypePairRelationsInner


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**source_vertex** | **str** |  | 
**destination_vertex** | **str** |  | 
**relation** | **str** |  | [optional] 
**x_csr_params** | [**BaseEdgeTypeVertexTypePairRelationsInnerXCsrParams**](BaseEdgeTypeVertexTypePairRelationsInnerXCsrParams.md) |  | [optional] 

## Example

```python
from graphscope.flex.rest.models.base_edge_type_vertex_type_pair_relations_inner import BaseEdgeTypeVertexTypePairRelationsInner

# TODO update the JSON string below
json = "{}"
# create an instance of BaseEdgeTypeVertexTypePairRelationsInner from a JSON string
base_edge_type_vertex_type_pair_relations_inner_instance = BaseEdgeTypeVertexTypePairRelationsInner.from_json(json)
# print the JSON string representation of the object
print(BaseEdgeTypeVertexTypePairRelationsInner.to_json())

# convert the object into a dict
base_edge_type_vertex_type_pair_relations_inner_dict = base_edge_type_vertex_type_pair_relations_inner_instance.to_dict()
# create an instance of BaseEdgeTypeVertexTypePairRelationsInner from a dict
base_edge_type_vertex_type_pair_relations_inner_from_dict = BaseEdgeTypeVertexTypePairRelationsInner.from_dict(base_edge_type_vertex_type_pair_relations_inner_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


