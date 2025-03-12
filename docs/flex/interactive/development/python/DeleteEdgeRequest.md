# DeleteEdgeRequest


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**edge_label** | **str** | The label name of the edge. | [optional] 
**src_label** | **str** | The label name of the source vertex. | [optional] 
**dst_label** | **str** | The label name of the destination vertex. | [optional] 
**src_primary_key_values** | [**List[ModelProperty]**](ModelProperty.md) | Primary key values for the source vertex. | [optional] 
**dst_primary_key_values** | [**List[ModelProperty]**](ModelProperty.md) | Primary key values for the destination vertex. | [optional] 
**properties** | [**List[ModelProperty]**](ModelProperty.md) | The properties of the edge. If the edge type has primary key, it should be included in the properties. | [optional] 

## Example

```python
from gs_interactive.models.delete_edge_request import DeleteEdgeRequest

# TODO update the JSON string below
json = "{}"
# create an instance of DeleteEdgeRequest from a JSON string
delete_edge_request_instance = DeleteEdgeRequest.from_json(json)
# print the JSON string representation of the object
print DeleteEdgeRequest.to_json()

# convert the object into a dict
delete_edge_request_dict = delete_edge_request_instance.to_dict()
# create an instance of DeleteEdgeRequest from a dict
delete_edge_request_form_dict = delete_edge_request.from_dict(delete_edge_request_dict)
```
[[Back to Model list]](python_sdk.md#documentation-for-data-structures) [[Back to API list]](python_sdk.md#documentation-for-service-apis) [[Back to python_sdk]](python_sdk.md)


