# DataloadingJobConfigLoadingConfig


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**import_option** | **str** |  | [optional] 
**format** | [**DataloadingJobConfigLoadingConfigFormat**](DataloadingJobConfigLoadingConfigFormat.md) |  | [optional] 

## Example

```python
from graphscope.flex.rest.models.dataloading_job_config_loading_config import DataloadingJobConfigLoadingConfig

# TODO update the JSON string below
json = "{}"
# create an instance of DataloadingJobConfigLoadingConfig from a JSON string
dataloading_job_config_loading_config_instance = DataloadingJobConfigLoadingConfig.from_json(json)
# print the JSON string representation of the object
print(DataloadingJobConfigLoadingConfig.to_json())

# convert the object into a dict
dataloading_job_config_loading_config_dict = dataloading_job_config_loading_config_instance.to_dict()
# create an instance of DataloadingJobConfigLoadingConfig from a dict
dataloading_job_config_loading_config_from_dict = DataloadingJobConfigLoadingConfig.from_dict(dataloading_job_config_loading_config_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


