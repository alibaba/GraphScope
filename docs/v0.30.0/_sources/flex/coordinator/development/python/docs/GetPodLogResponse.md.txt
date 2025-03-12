# GetPodLogResponse


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**log** | **Dict[str, object]** |  | 

## Example

```python
from graphscope.flex.rest.models.get_pod_log_response import GetPodLogResponse

# TODO update the JSON string below
json = "{}"
# create an instance of GetPodLogResponse from a JSON string
get_pod_log_response_instance = GetPodLogResponse.from_json(json)
# print the JSON string representation of the object
print(GetPodLogResponse.to_json())

# convert the object into a dict
get_pod_log_response_dict = get_pod_log_response_instance.to_dict()
# create an instance of GetPodLogResponse from a dict
get_pod_log_response_from_dict = GetPodLogResponse.from_dict(get_pod_log_response_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


