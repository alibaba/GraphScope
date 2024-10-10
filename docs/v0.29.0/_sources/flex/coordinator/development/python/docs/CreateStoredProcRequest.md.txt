# CreateStoredProcRequest


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**name** | **str** |  | 
**description** | **str** |  | [optional] 
**type** | **str** |  | 
**query** | **str** |  | 

## Example

```python
from graphscope.flex.rest.models.create_stored_proc_request import CreateStoredProcRequest

# TODO update the JSON string below
json = "{}"
# create an instance of CreateStoredProcRequest from a JSON string
create_stored_proc_request_instance = CreateStoredProcRequest.from_json(json)
# print the JSON string representation of the object
print(CreateStoredProcRequest.to_json())

# convert the object into a dict
create_stored_proc_request_dict = create_stored_proc_request_instance.to_dict()
# create an instance of CreateStoredProcRequest from a dict
create_stored_proc_request_from_dict = CreateStoredProcRequest.from_dict(create_stored_proc_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


