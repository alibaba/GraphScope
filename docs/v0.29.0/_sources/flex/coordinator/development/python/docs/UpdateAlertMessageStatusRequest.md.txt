# UpdateAlertMessageStatusRequest


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**message_ids** | **List[str]** |  | 
**status** | **str** |  | 

## Example

```python
from graphscope.flex.rest.models.update_alert_message_status_request import UpdateAlertMessageStatusRequest

# TODO update the JSON string below
json = "{}"
# create an instance of UpdateAlertMessageStatusRequest from a JSON string
update_alert_message_status_request_instance = UpdateAlertMessageStatusRequest.from_json(json)
# print the JSON string representation of the object
print(UpdateAlertMessageStatusRequest.to_json())

# convert the object into a dict
update_alert_message_status_request_dict = update_alert_message_status_request_instance.to_dict()
# create an instance of UpdateAlertMessageStatusRequest from a dict
update_alert_message_status_request_from_dict = UpdateAlertMessageStatusRequest.from_dict(update_alert_message_status_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


