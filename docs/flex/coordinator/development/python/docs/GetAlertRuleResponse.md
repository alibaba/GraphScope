# GetAlertRuleResponse


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**name** | **str** |  | 
**severity** | **str** |  | 
**metric_type** | **str** |  | 
**conditions_description** | **str** |  | 
**frequency** | **int** | (mins) | 
**enable** | **bool** |  | 
**id** | **str** |  | 

## Example

```python
from graphscope.flex.rest.models.get_alert_rule_response import GetAlertRuleResponse

# TODO update the JSON string below
json = "{}"
# create an instance of GetAlertRuleResponse from a JSON string
get_alert_rule_response_instance = GetAlertRuleResponse.from_json(json)
# print the JSON string representation of the object
print(GetAlertRuleResponse.to_json())

# convert the object into a dict
get_alert_rule_response_dict = get_alert_rule_response_instance.to_dict()
# create an instance of GetAlertRuleResponse from a dict
get_alert_rule_response_from_dict = GetAlertRuleResponse.from_dict(get_alert_rule_response_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


