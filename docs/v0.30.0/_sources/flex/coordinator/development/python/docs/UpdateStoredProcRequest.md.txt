# UpdateStoredProcRequest


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**description** | **str** |  | 

## Example

```python
from graphscope.flex.rest.models.update_stored_proc_request import UpdateStoredProcRequest

# TODO update the JSON string below
json = "{}"
# create an instance of UpdateStoredProcRequest from a JSON string
update_stored_proc_request_instance = UpdateStoredProcRequest.from_json(json)
# print the JSON string representation of the object
print(UpdateStoredProcRequest.to_json())

# convert the object into a dict
update_stored_proc_request_dict = update_stored_proc_request_instance.to_dict()
# create an instance of UpdateStoredProcRequest from a dict
update_stored_proc_request_from_dict = UpdateStoredProcRequest.from_dict(update_stored_proc_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


