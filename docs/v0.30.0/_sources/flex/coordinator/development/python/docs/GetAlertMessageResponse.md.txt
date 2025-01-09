# GetAlertMessageResponse


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**id** | **str** | Generated in server side | 
**alert_name** | **str** |  | 
**severity** | **str** |  | 
**metric_type** | **str** |  | 
**target** | **List[str]** |  | 
**trigger_time** | **str** |  | 
**status** | **str** |  | 
**message** | **str** |  | 

## Example

```python
from graphscope.flex.rest.models.get_alert_message_response import GetAlertMessageResponse

# TODO update the JSON string below
json = "{}"
# create an instance of GetAlertMessageResponse from a JSON string
get_alert_message_response_instance = GetAlertMessageResponse.from_json(json)
# print the JSON string representation of the object
print(GetAlertMessageResponse.to_json())

# convert the object into a dict
get_alert_message_response_dict = get_alert_message_response_instance.to_dict()
# create an instance of GetAlertMessageResponse from a dict
get_alert_message_response_from_dict = GetAlertMessageResponse.from_dict(get_alert_message_response_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


