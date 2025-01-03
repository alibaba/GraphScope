# DataloadingJobConfigLoadingConfigFormat


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**type** | **str** |  | [optional] 
**metadata** | **Dict[str, object]** |  | [optional] 

## Example

```python
from graphscope.flex.rest.models.dataloading_job_config_loading_config_format import DataloadingJobConfigLoadingConfigFormat

# TODO update the JSON string below
json = "{}"
# create an instance of DataloadingJobConfigLoadingConfigFormat from a JSON string
dataloading_job_config_loading_config_format_instance = DataloadingJobConfigLoadingConfigFormat.from_json(json)
# print the JSON string representation of the object
print(DataloadingJobConfigLoadingConfigFormat.to_json())

# convert the object into a dict
dataloading_job_config_loading_config_format_dict = dataloading_job_config_loading_config_format_instance.to_dict()
# create an instance of DataloadingJobConfigLoadingConfigFormat from a dict
dataloading_job_config_loading_config_format_from_dict = DataloadingJobConfigLoadingConfigFormat.from_dict(dataloading_job_config_loading_config_format_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


