# DataloadingJobConfigEdgesInner


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**type_name** | **str** |  | [optional] 
**source_vertex** | **str** |  | [optional] 
**destination_vertex** | **str** |  | [optional] 

## Example

```python
from graphscope.flex.rest.models.dataloading_job_config_edges_inner import DataloadingJobConfigEdgesInner

# TODO update the JSON string below
json = "{}"
# create an instance of DataloadingJobConfigEdgesInner from a JSON string
dataloading_job_config_edges_inner_instance = DataloadingJobConfigEdgesInner.from_json(json)
# print the JSON string representation of the object
print(DataloadingJobConfigEdgesInner.to_json())

# convert the object into a dict
dataloading_job_config_edges_inner_dict = dataloading_job_config_edges_inner_instance.to_dict()
# create an instance of DataloadingJobConfigEdgesInner from a dict
dataloading_job_config_edges_inner_from_dict = DataloadingJobConfigEdgesInner.from_dict(dataloading_job_config_edges_inner_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


