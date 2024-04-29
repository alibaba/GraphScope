# BaseEdgeType


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**type_name** | **str** |  | [optional] 
**vertex_type_pair_relations** | [**List[BaseEdgeTypeVertexTypePairRelationsInner]**](BaseEdgeTypeVertexTypePairRelationsInner.md) |  | [optional] 

## Example

```python
from interactive_sdk.models.base_edge_type import BaseEdgeType

# TODO update the JSON string below
json = "{}"
# create an instance of BaseEdgeType from a JSON string
base_edge_type_instance = BaseEdgeType.from_json(json)
# print the JSON string representation of the object
print BaseEdgeType.to_json()

# convert the object into a dict
base_edge_type_dict = base_edge_type_instance.to_dict()
# create an instance of BaseEdgeType from a dict
base_edge_type_form_dict = base_edge_type.from_dict(base_edge_type_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


