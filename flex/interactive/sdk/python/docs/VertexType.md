# VertexType


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**type_id** | **int** |  | [optional] 
**type_name** | **str** |  | [optional] 
**properties** | [**List[PropertyMeta]**](PropertyMeta.md) |  | [optional] 
**primary_keys** | **List[str]** |  | [optional] 

## Example

```python
from interactive_sdk.models.vertex_type import VertexType

# TODO update the JSON string below
json = "{}"
# create an instance of VertexType from a JSON string
vertex_type_instance = VertexType.from_json(json)
# print the JSON string representation of the object
print VertexType.to_json()

# convert the object into a dict
vertex_type_dict = vertex_type_instance.to_dict()
# create an instance of VertexType from a dict
vertex_type_form_dict = vertex_type.from_dict(vertex_type_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


