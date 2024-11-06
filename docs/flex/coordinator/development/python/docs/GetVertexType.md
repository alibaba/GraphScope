# GetVertexType


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**type_name** | **str** |  | 
**primary_keys** | **List[str]** |  | 
**x_csr_params** | [**BaseVertexTypeXCsrParams**](BaseVertexTypeXCsrParams.md) |  | [optional] 
**type_id** | **int** |  | [optional] 
**properties** | [**List[GetPropertyMeta]**](GetPropertyMeta.md) |  | 
**description** | **str** |  | [optional] 

## Example

```python
from graphscope.flex.rest.models.get_vertex_type import GetVertexType

# TODO update the JSON string below
json = "{}"
# create an instance of GetVertexType from a JSON string
get_vertex_type_instance = GetVertexType.from_json(json)
# print the JSON string representation of the object
print(GetVertexType.to_json())

# convert the object into a dict
get_vertex_type_dict = get_vertex_type_instance.to_dict()
# create an instance of GetVertexType from a dict
get_vertex_type_from_dict = GetVertexType.from_dict(get_vertex_type_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


