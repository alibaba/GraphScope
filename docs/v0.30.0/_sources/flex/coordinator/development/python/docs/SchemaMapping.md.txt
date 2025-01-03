# SchemaMapping


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**vertex_mappings** | [**List[VertexMapping]**](VertexMapping.md) |  | 
**edge_mappings** | [**List[EdgeMapping]**](EdgeMapping.md) |  | 

## Example

```python
from graphscope.flex.rest.models.schema_mapping import SchemaMapping

# TODO update the JSON string below
json = "{}"
# create an instance of SchemaMapping from a JSON string
schema_mapping_instance = SchemaMapping.from_json(json)
# print the JSON string representation of the object
print(SchemaMapping.to_json())

# convert the object into a dict
schema_mapping_dict = schema_mapping_instance.to_dict()
# create an instance of SchemaMapping from a dict
schema_mapping_from_dict = SchemaMapping.from_dict(schema_mapping_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


