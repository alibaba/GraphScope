# RunningDeploymentStatus


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**cluster_type** | **str** |  | 
**nodes** | [**List[NodeStatus]**](NodeStatus.md) |  | [optional] 
**pods** | **Dict[str, List[PodStatus]]** |  | [optional] 

## Example

```python
from graphscope.flex.rest.models.running_deployment_status import RunningDeploymentStatus

# TODO update the JSON string below
json = "{}"
# create an instance of RunningDeploymentStatus from a JSON string
running_deployment_status_instance = RunningDeploymentStatus.from_json(json)
# print the JSON string representation of the object
print(RunningDeploymentStatus.to_json())

# convert the object into a dict
running_deployment_status_dict = running_deployment_status_instance.to_dict()
# create an instance of RunningDeploymentStatus from a dict
running_deployment_status_from_dict = RunningDeploymentStatus.from_dict(running_deployment_status_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


