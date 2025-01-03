# CreateAlertRuleRequest


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**name** | **str** |  | 
**severity** | **str** |  | 
**metric_type** | **str** |  | 
**conditions_description** | **str** |  | 
**frequency** | **int** | (mins) | 
**enable** | **bool** |  | 

## Example

```python
from graphscope.flex.rest.models.create_alert_rule_request import CreateAlertRuleRequest

# TODO update the JSON string below
json = "{}"
# create an instance of CreateAlertRuleRequest from a JSON string
create_alert_rule_request_instance = CreateAlertRuleRequest.from_json(json)
# print the JSON string representation of the object
print(CreateAlertRuleRequest.to_json())

# convert the object into a dict
create_alert_rule_request_dict = create_alert_rule_request_instance.to_dict()
# create an instance of CreateAlertRuleRequest from a dict
create_alert_rule_request_from_dict = CreateAlertRuleRequest.from_dict(create_alert_rule_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


