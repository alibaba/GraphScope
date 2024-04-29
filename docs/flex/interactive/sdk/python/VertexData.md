# VertexData


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**label** | **str** |  | 
**values** | [**List[ModelProperty]**](ModelProperty.md) |  | [optional] 

## Example

```python
from interactive_sdk.models.vertex_data import VertexData

# TODO update the JSON string below
json = "{}"
# create an instance of VertexData from a JSON string
vertex_data_instance = VertexData.from_json(json)
# print the JSON string representation of the object
print VertexData.to_json()

# convert the object into a dict
vertex_data_dict = vertex_data_instance.to_dict()
# create an instance of VertexData from a dict
vertex_data_form_dict = vertex_data.from_dict(vertex_data_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


