# EdgeMappingTypeTriplet

source label -> [edge label] -> destination label

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**edge** | **str** |  | 
**source_vertex** | **str** |  | 
**destination_vertex** | **str** |  | 

## Example

```python
from graphscope.flex.rest.models.edge_mapping_type_triplet import EdgeMappingTypeTriplet

# TODO update the JSON string below
json = "{}"
# create an instance of EdgeMappingTypeTriplet from a JSON string
edge_mapping_type_triplet_instance = EdgeMappingTypeTriplet.from_json(json)
# print the JSON string representation of the object
print(EdgeMappingTypeTriplet.to_json())

# convert the object into a dict
edge_mapping_type_triplet_dict = edge_mapping_type_triplet_instance.to_dict()
# create an instance of EdgeMappingTypeTriplet from a dict
edge_mapping_type_triplet_from_dict = EdgeMappingTypeTriplet.from_dict(edge_mapping_type_triplet_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


