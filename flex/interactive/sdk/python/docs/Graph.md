# Graph


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**name** | **str** |  | [optional] 
**store_type** | **str** |  | [optional] 
**stored_procedures** | [**GraphStoredProcedures**](GraphStoredProcedures.md) |  | [optional] 
**var_schema** | [**GraphSchema**](GraphSchema.md) |  | [optional] 

## Example

```python
from interactive_sdk.models.graph import Graph

# TODO update the JSON string below
json = "{}"
# create an instance of Graph from a JSON string
graph_instance = Graph.from_json(json)
# print the JSON string representation of the object
print Graph.to_json()

# convert the object into a dict
graph_dict = graph_instance.to_dict()
# create an instance of Graph from a dict
graph_form_dict = graph.from_dict(graph_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


