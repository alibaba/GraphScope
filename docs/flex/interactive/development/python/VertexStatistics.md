# VertexStatistics


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**type_id** | **int** |  | [optional] 
**type_name** | **str** |  | [optional] 
**count** | **int** |  | [optional] 

## Example

```python
from interactive_sdk.openapi.models.vertex_statistics import VertexStatistics

# TODO update the JSON string below
json = "{}"
# create an instance of VertexStatistics from a JSON string
vertex_statistics_instance = VertexStatistics.from_json(json)
# print the JSON string representation of the object
print(VertexStatistics.to_json())

# convert the object into a dict
vertex_statistics_dict = vertex_statistics_instance.to_dict()
# create an instance of VertexStatistics from a dict
vertex_statistics_from_dict = VertexStatistics.from_dict(vertex_statistics_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


