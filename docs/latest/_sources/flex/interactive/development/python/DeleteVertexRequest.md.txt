# DeleteVertexRequest


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**label** | **str** | The label name of the vertex. | [optional] 
**primary_key_values** | [**List[ModelProperty]**](ModelProperty.md) | Primary key values for the vertex. | [optional] 

## Example

```python
from gs_interactive.models.delete_vertex_request import DeleteVertexRequest

# TODO update the JSON string below
json = "{}"
# create an instance of DeleteVertexRequest from a JSON string
delete_vertex_request_instance = DeleteVertexRequest.from_json(json)
# print the JSON string representation of the object
print DeleteVertexRequest.to_json()

# convert the object into a dict
delete_vertex_request_dict = delete_vertex_request_instance.to_dict()
# create an instance of DeleteVertexRequest from a dict
delete_vertex_request_form_dict = delete_vertex_request.from_dict(delete_vertex_request_dict)
```
[[Back to Model list]](python_sdk.md#documentation-for-data-structures) [[Back to API list]](python_sdk.md#documentation-for-service-apis) [[Back to python_sdk]](python_sdk.md)


