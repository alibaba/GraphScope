# BaseVertexType


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**type_name** | **str** |  | [optional] 
**primary_keys** | **List[str]** |  | [optional] 
**x_csr_params** | [**BaseVertexTypeXCsrParams**](BaseVertexTypeXCsrParams.md) |  | [optional] 

## Example

```python
from interactive_sdk.models.base_vertex_type import BaseVertexType

# TODO update the JSON string below
json = "{}"
# create an instance of BaseVertexType from a JSON string
base_vertex_type_instance = BaseVertexType.from_json(json)
# print the JSON string representation of the object
print BaseVertexType.to_json()

# convert the object into a dict
base_vertex_type_dict = base_vertex_type_instance.to_dict()
# create an instance of BaseVertexType from a dict
base_vertex_type_form_dict = base_vertex_type.from_dict(base_vertex_type_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


