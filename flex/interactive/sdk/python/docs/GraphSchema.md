# GraphSchema


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**vertex_types** | [**List[VertexType]**](VertexType.md) |  | [optional] 
**edge_types** | [**List[EdgeType]**](EdgeType.md) |  | [optional] 

## Example

```python
from interactive_sdk.models.graph_schema import GraphSchema

# TODO update the JSON string below
json = "{}"
# create an instance of GraphSchema from a JSON string
graph_schema_instance = GraphSchema.from_json(json)
# print the JSON string representation of the object
print GraphSchema.to_json()

# convert the object into a dict
graph_schema_dict = graph_schema_instance.to_dict()
# create an instance of GraphSchema from a dict
graph_schema_form_dict = graph_schema.from_dict(graph_schema_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


