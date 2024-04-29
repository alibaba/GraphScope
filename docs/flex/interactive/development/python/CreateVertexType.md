# CreateVertexType


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**type_name** | **str** |  | [optional] 
**primary_keys** | **List[str]** |  | [optional] 
**x_csr_params** | [**BaseVertexTypeXCsrParams**](BaseVertexTypeXCsrParams.md) |  | [optional] 
**properties** | [**List[CreatePropertyMeta]**](CreatePropertyMeta.md) |  | [optional] 

## Example

```python
from interactive_sdk.models.create_vertex_type import CreateVertexType

# TODO update the JSON string below
json = "{}"
# create an instance of CreateVertexType from a JSON string
create_vertex_type_instance = CreateVertexType.from_json(json)
# print the JSON string representation of the object
print CreateVertexType.to_json()

# convert the object into a dict
create_vertex_type_dict = create_vertex_type_instance.to_dict()
# create an instance of CreateVertexType from a dict
create_vertex_type_form_dict = create_vertex_type.from_dict(create_vertex_type_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


