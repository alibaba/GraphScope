# CreateEdgeType


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**type_name** | **str** |  | 
**vertex_type_pair_relations** | [**List[BaseEdgeTypeVertexTypePairRelationsInner]**](BaseEdgeTypeVertexTypePairRelationsInner.md) |  | 
**directed** | **bool** |  | [optional] 
**primary_keys** | **List[str]** |  | [optional] 
**properties** | [**List[CreatePropertyMeta]**](CreatePropertyMeta.md) |  | [optional] 
**description** | **str** |  | [optional] 

## Example

```python
from graphscope.flex.rest.models.create_edge_type import CreateEdgeType

# TODO update the JSON string below
json = "{}"
# create an instance of CreateEdgeType from a JSON string
create_edge_type_instance = CreateEdgeType.from_json(json)
# print the JSON string representation of the object
print(CreateEdgeType.to_json())

# convert the object into a dict
create_edge_type_dict = create_edge_type_instance.to_dict()
# create an instance of CreateEdgeType from a dict
create_edge_type_from_dict = CreateEdgeType.from_dict(create_edge_type_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


