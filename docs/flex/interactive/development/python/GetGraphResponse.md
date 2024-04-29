# GetGraphResponse


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**id** | **str** |  | [optional] 
**name** | **str** |  | [optional] 
**description** | **str** |  | [optional] 
**store_type** | **str** |  | [optional] 
**creation_time** | **int** |  | [optional] 
**data_update_time** | **int** |  | [optional] 
**stored_procedures** | [**List[GetProcedureResponse]**](GetProcedureResponse.md) |  | [optional] 
**var_schema** | [**GetGraphSchemaResponse**](GetGraphSchemaResponse.md) |  | [optional] 
**data_import_config** | [**SchemaMapping**](SchemaMapping.md) |  | [optional] 

## Example

```python
from interactive_sdk.models.get_graph_response import GetGraphResponse

# TODO update the JSON string below
json = "{}"
# create an instance of GetGraphResponse from a JSON string
get_graph_response_instance = GetGraphResponse.from_json(json)
# print the JSON string representation of the object
print GetGraphResponse.to_json()

# convert the object into a dict
get_graph_response_dict = get_graph_response_instance.to_dict()
# create an instance of GetGraphResponse from a dict
get_graph_response_form_dict = get_graph_response.from_dict(get_graph_response_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


