# CreateGraphRequest


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**name** | **str** |  | [optional] 
**description** | **str** |  | [optional] 
**stored_procedures** | [**List[CreateStoredProcRequest]**](CreateStoredProcRequest.md) |  | [optional] 
**var_schema** | [**CreateGraphSchemaRequest**](CreateGraphSchemaRequest.md) |  | [optional] 

## Example

```python
from graphscope.flex.rest.models.create_graph_request import CreateGraphRequest

# TODO update the JSON string below
json = "{}"
# create an instance of CreateGraphRequest from a JSON string
create_graph_request_instance = CreateGraphRequest.from_json(json)
# print the JSON string representation of the object
print(CreateGraphRequest.to_json())

# convert the object into a dict
create_graph_request_dict = create_graph_request_instance.to_dict()
# create an instance of CreateGraphRequest from a dict
create_graph_request_from_dict = CreateGraphRequest.from_dict(create_graph_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


