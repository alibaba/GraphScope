# DataloadingJobConfig


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**loading_config** | [**DataloadingJobConfigLoadingConfig**](DataloadingJobConfigLoadingConfig.md) |  | 
**vertices** | [**List[DataloadingJobConfigVerticesInner]**](DataloadingJobConfigVerticesInner.md) |  | 
**edges** | [**List[DataloadingJobConfigEdgesInner]**](DataloadingJobConfigEdgesInner.md) |  | 
**schedule** | **str** | format with &#39;2023-02-21 11:56:30&#39; | [optional] 
**repeat** | **str** |  | [optional] 

## Example

```python
from graphscope.flex.rest.models.dataloading_job_config import DataloadingJobConfig

# TODO update the JSON string below
json = "{}"
# create an instance of DataloadingJobConfig from a JSON string
dataloading_job_config_instance = DataloadingJobConfig.from_json(json)
# print the JSON string representation of the object
print(DataloadingJobConfig.to_json())

# convert the object into a dict
dataloading_job_config_dict = dataloading_job_config_instance.to_dict()
# create an instance of DataloadingJobConfig from a dict
dataloading_job_config_from_dict = DataloadingJobConfig.from_dict(dataloading_job_config_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


