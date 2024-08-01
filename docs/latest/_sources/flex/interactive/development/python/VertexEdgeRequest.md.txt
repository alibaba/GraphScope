# VertexEdgeRequest


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**vertex_request** | [**List[VertexRequest]**](VertexRequest.md) |  | 
**edge_request** | [**List[EdgeRequest]**](EdgeRequest.md) |  | 

## Example

```python
from gs_interactive.models.vertex_edge_request import VertexEdgeRequest

# TODO update the JSON string below
json = "{}"
# create an instance of VertexEdgeRequest from a JSON string
vertex_edge_request_instance = VertexEdgeRequest.from_json(json)
# print the JSON string representation of the object
print(VertexEdgeRequest.to_json())

# convert the object into a dict
vertex_edge_request_dict = vertex_edge_request_instance.to_dict()
# create an instance of VertexEdgeRequest from a dict
vertex_edge_request_from_dict = VertexEdgeRequest.from_dict(vertex_edge_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


