# GetGraphResponse


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**id** | **str** |  | 
**name** | **str** |  | 
**description** | **str** |  | [optional] 
**store_type** | **str** |  | [optional] 
**creation_time** | **str** |  | 
**data_update_time** | **str** |  | 
**schema_update_time** | **str** |  | 
**stored_procedures** | [**List[GetStoredProcResponse]**](GetStoredProcResponse.md) |  | [optional] 
**var_schema** | [**GetGraphSchemaResponse**](GetGraphSchemaResponse.md) |  | 

## Example

```python
from graphscope.flex.rest.models.get_graph_response import GetGraphResponse

# TODO update the JSON string below
json = "{}"
# create an instance of GetGraphResponse from a JSON string
get_graph_response_instance = GetGraphResponse.from_json(json)
# print the JSON string representation of the object
print(GetGraphResponse.to_json())

# convert the object into a dict
get_graph_response_dict = get_graph_response_instance.to_dict()
# create an instance of GetGraphResponse from a dict
get_graph_response_from_dict = GetGraphResponse.from_dict(get_graph_response_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


