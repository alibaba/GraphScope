# VertexTypePairStatistics


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**source_vertex** | **str** |  | 
**destination_vertex** | **str** |  | 
**count** | **int** |  | 

## Example

```python
from interactive_sdk.openapi.models.vertex_type_pair_statistics import VertexTypePairStatistics

# TODO update the JSON string below
json = "{}"
# create an instance of VertexTypePairStatistics from a JSON string
vertex_type_pair_statistics_instance = VertexTypePairStatistics.from_json(json)
# print the JSON string representation of the object
print(VertexTypePairStatistics.to_json())

# convert the object into a dict
vertex_type_pair_statistics_dict = vertex_type_pair_statistics_instance.to_dict()
# create an instance of VertexTypePairStatistics from a dict
vertex_type_pair_statistics_from_dict = VertexTypePairStatistics.from_dict(vertex_type_pair_statistics_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


