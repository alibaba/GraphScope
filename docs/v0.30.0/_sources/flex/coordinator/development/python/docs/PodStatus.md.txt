# PodStatus


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**name** | **str** |  | 
**image** | **List[str]** |  | 
**labels** | **Dict[str, object]** |  | 
**node** | **str** |  | 
**status** | **str** |  | 
**restart_count** | **int** |  | 
**cpu_usage** | **int** |  | 
**memory_usage** | **int** |  | 
**timestamp** | **str** |  | [optional] 
**creation_time** | **str** |  | 
**component_belong_to** | **str** |  | [optional] 

## Example

```python
from graphscope.flex.rest.models.pod_status import PodStatus

# TODO update the JSON string below
json = "{}"
# create an instance of PodStatus from a JSON string
pod_status_instance = PodStatus.from_json(json)
# print the JSON string representation of the object
print(PodStatus.to_json())

# convert the object into a dict
pod_status_dict = pod_status_instance.to_dict()
# create an instance of PodStatus from a dict
pod_status_from_dict = PodStatus.from_dict(pod_status_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


