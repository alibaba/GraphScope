# ServiceMetrics


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**snapshot_id** | **str** |  | [optional] 

## Example

```python
from gs_interactive.models.service_metrics import ServiceMetrics

# TODO update the JSON string below
json = "{}"
# create an instance of ServiceMetrics from a JSON string
service_metrics_instance = ServiceMetrics.from_json(json)
# print the JSON string representation of the object
print ServiceMetrics.to_json()

# convert the object into a dict
service_metrics_dict = service_metrics_instance.to_dict()
# create an instance of ServiceMetrics from a dict
service_metrics_form_dict = service_metrics.from_dict(service_metrics_dict)
```
[[Back to Model list]](python_sdk.md#documentation-for-data-structures) [[Back to API list]](python_sdk.md#documentation-for-service-apis) [[Back to python_sdk]](python_sdk.md)


