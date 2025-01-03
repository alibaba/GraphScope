# GetStoredProcResponse


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**name** | **str** |  | 
**description** | **str** |  | [optional] 
**type** | **str** |  | 
**query** | **str** |  | 
**id** | **str** |  | 
**library** | **str** |  | 
**params** | [**List[Parameter]**](Parameter.md) |  | 
**returns** | [**List[Parameter]**](Parameter.md) |  | 
**option** | **Dict[str, object]** |  | [optional] 
**bound_graph** | **str** |  | 
**runnable** | **bool** |  | 

## Example

```python
from graphscope.flex.rest.models.get_stored_proc_response import GetStoredProcResponse

# TODO update the JSON string below
json = "{}"
# create an instance of GetStoredProcResponse from a JSON string
get_stored_proc_response_instance = GetStoredProcResponse.from_json(json)
# print the JSON string representation of the object
print(GetStoredProcResponse.to_json())

# convert the object into a dict
get_stored_proc_response_dict = get_stored_proc_response_instance.to_dict()
# create an instance of GetStoredProcResponse from a dict
get_stored_proc_response_from_dict = GetStoredProcResponse.from_dict(get_stored_proc_response_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


