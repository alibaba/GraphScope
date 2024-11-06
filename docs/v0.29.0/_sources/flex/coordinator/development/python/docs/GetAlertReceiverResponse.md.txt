# GetAlertReceiverResponse


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**type** | **str** |  | 
**webhook_url** | **str** |  | 
**at_user_ids** | **List[str]** |  | 
**is_at_all** | **bool** |  | 
**enable** | **bool** |  | 
**id** | **str** |  | 
**message** | **str** | Error message generated in server side | 

## Example

```python
from graphscope.flex.rest.models.get_alert_receiver_response import GetAlertReceiverResponse

# TODO update the JSON string below
json = "{}"
# create an instance of GetAlertReceiverResponse from a JSON string
get_alert_receiver_response_instance = GetAlertReceiverResponse.from_json(json)
# print the JSON string representation of the object
print(GetAlertReceiverResponse.to_json())

# convert the object into a dict
get_alert_receiver_response_dict = get_alert_receiver_response_instance.to_dict()
# create an instance of GetAlertReceiverResponse from a dict
get_alert_receiver_response_from_dict = GetAlertReceiverResponse.from_dict(get_alert_receiver_response_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


