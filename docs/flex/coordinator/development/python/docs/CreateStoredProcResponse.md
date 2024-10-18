# CreateStoredProcResponse


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**stored_procedure_id** | **str** |  | 

## Example

```python
from graphscope.flex.rest.models.create_stored_proc_response import CreateStoredProcResponse

# TODO update the JSON string below
json = "{}"
# create an instance of CreateStoredProcResponse from a JSON string
create_stored_proc_response_instance = CreateStoredProcResponse.from_json(json)
# print the JSON string representation of the object
print(CreateStoredProcResponse.to_json())

# convert the object into a dict
create_stored_proc_response_dict = create_stored_proc_response_instance.to_dict()
# create an instance of CreateStoredProcResponse from a dict
create_stored_proc_response_from_dict = CreateStoredProcResponse.from_dict(create_stored_proc_response_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


