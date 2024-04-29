# GetEdgeType


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**type_name** | **str** |  | [optional] 
**vertex_type_pair_relations** | [**List[BaseEdgeTypeVertexTypePairRelationsInner]**](BaseEdgeTypeVertexTypePairRelationsInner.md) |  | [optional] 
**type_id** | **int** |  | [optional] 
**description** | **str** |  | [optional] 
**properties** | [**List[GetPropertyMeta]**](GetPropertyMeta.md) |  | [optional] 

## Example

```python
from interactive_sdk.models.get_edge_type import GetEdgeType

# TODO update the JSON string below
json = "{}"
# create an instance of GetEdgeType from a JSON string
get_edge_type_instance = GetEdgeType.from_json(json)
# print the JSON string representation of the object
print GetEdgeType.to_json()

# convert the object into a dict
get_edge_type_dict = get_edge_type_instance.to_dict()
# create an instance of GetEdgeType from a dict
get_edge_type_form_dict = get_edge_type.from_dict(get_edge_type_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


