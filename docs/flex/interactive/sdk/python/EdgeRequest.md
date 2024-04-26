# EdgeRequest


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**src_label** | **str** |  | 
**dst_label** | **str** |  | 
**edge_label** | **str** |  | 
**src_primary_key_value** | **object** |  | 
**dst_primary_key_value** | **object** |  | 
**properties** | [**List[ModelProperty]**](ModelProperty.md) |  | [optional] 

## Example

```python
from interactive_sdk.models.edge_request import EdgeRequest

# TODO update the JSON string below
json = "{}"
# create an instance of EdgeRequest from a JSON string
edge_request_instance = EdgeRequest.from_json(json)
# print the JSON string representation of the object
print EdgeRequest.to_json()

# convert the object into a dict
edge_request_dict = edge_request_instance.to_dict()
# create an instance of EdgeRequest from a dict
edge_request_form_dict = edge_request.from_dict(edge_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


