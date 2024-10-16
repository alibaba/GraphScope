# RunningDeploymentInfo


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**instance_name** | **str** |  | 
**cluster_type** | **str** |  | 
**version** | **str** |  | 
**creation_time** | **str** |  | 
**frontend** | **str** |  | 
**engine** | **str** |  | 
**storage** | **str** |  | 

## Example

```python
from graphscope.flex.rest.models.running_deployment_info import RunningDeploymentInfo

# TODO update the JSON string below
json = "{}"
# create an instance of RunningDeploymentInfo from a JSON string
running_deployment_info_instance = RunningDeploymentInfo.from_json(json)
# print the JSON string representation of the object
print(RunningDeploymentInfo.to_json())

# convert the object into a dict
running_deployment_info_dict = running_deployment_info_instance.to_dict()
# create an instance of RunningDeploymentInfo from a dict
running_deployment_info_from_dict = RunningDeploymentInfo.from_dict(running_deployment_info_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


