# ServiceStatus


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**status** | **str** |  | [optional] 
**graph_id** | **str** |  | [optional] 
**bolt_port** | **int** |  | [optional] 
**hqps_port** | **int** |  | [optional] 
**gremlin_port** | **int** |  | [optional] 

## Example

```python
from interactive_sdk.models.service_status import ServiceStatus

# TODO update the JSON string below
json = "{}"
# create an instance of ServiceStatus from a JSON string
service_status_instance = ServiceStatus.from_json(json)
# print the JSON string representation of the object
print ServiceStatus.to_json()

# convert the object into a dict
service_status_dict = service_status_instance.to_dict()
# create an instance of ServiceStatus from a dict
service_status_form_dict = service_status.from_dict(service_status_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


