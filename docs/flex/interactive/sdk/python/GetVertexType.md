# GetVertexType


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**type_name** | **str** |  | [optional] 
**primary_keys** | **List[str]** |  | [optional] 
**x_csr_params** | [**BaseVertexTypeXCsrParams**](BaseVertexTypeXCsrParams.md) |  | [optional] 
**type_id** | **int** |  | [optional] 
**properties** | [**List[GetPropertyMeta]**](GetPropertyMeta.md) |  | [optional] 
**description** | **str** |  | [optional] 

## Example

```python
from interactive_sdk.models.get_vertex_type import GetVertexType

# TODO update the JSON string below
json = "{}"
# create an instance of GetVertexType from a JSON string
get_vertex_type_instance = GetVertexType.from_json(json)
# print the JSON string representation of the object
print GetVertexType.to_json()

# convert the object into a dict
get_vertex_type_dict = get_vertex_type_instance.to_dict()
# create an instance of GetVertexType from a dict
get_vertex_type_form_dict = get_vertex_type.from_dict(get_vertex_type_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


