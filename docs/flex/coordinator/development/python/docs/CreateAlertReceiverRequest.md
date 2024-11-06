# CreateAlertReceiverRequest


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**type** | **str** |  | 
**webhook_url** | **str** |  | 
**at_user_ids** | **List[str]** |  | 
**is_at_all** | **bool** |  | 
**enable** | **bool** |  | 

## Example

```python
from graphscope.flex.rest.models.create_alert_receiver_request import CreateAlertReceiverRequest

# TODO update the JSON string below
json = "{}"
# create an instance of CreateAlertReceiverRequest from a JSON string
create_alert_receiver_request_instance = CreateAlertReceiverRequest.from_json(json)
# print the JSON string representation of the object
print(CreateAlertReceiverRequest.to_json())

# convert the object into a dict
create_alert_receiver_request_dict = create_alert_receiver_request_instance.to_dict()
# create an instance of CreateAlertReceiverRequest from a dict
create_alert_receiver_request_from_dict = CreateAlertReceiverRequest.from_dict(create_alert_receiver_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


