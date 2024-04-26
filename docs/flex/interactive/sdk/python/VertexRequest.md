# VertexRequest


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**label** | **str** |  | 
**primary_key_value** | **object** |  | 
**properties** | [**PropertyArray**](PropertyArray.md) |  | [optional] 

## Example

```python
from interactive_sdk.models.vertex_request import VertexRequest

# TODO update the JSON string below
json = "{}"
# create an instance of VertexRequest from a JSON string
vertex_request_instance = VertexRequest.from_json(json)
# print the JSON string representation of the object
print VertexRequest.to_json()

# convert the object into a dict
vertex_request_dict = vertex_request_instance.to_dict()
# create an instance of VertexRequest from a dict
vertex_request_form_dict = vertex_request.from_dict(vertex_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


