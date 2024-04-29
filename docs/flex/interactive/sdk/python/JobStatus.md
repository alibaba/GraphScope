# JobStatus


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**id** | **str** |  | [optional] 
**type** | **str** |  | [optional] 
**status** | **str** |  | [optional] 
**start_time** | **int** |  | [optional] 
**end_time** | **int** |  | [optional] 
**log** | **str** | URL or log string | [optional] 
**detail** | **Dict[str, object]** |  | [optional] 

## Example

```python
from interactive_sdk.models.job_status import JobStatus

# TODO update the JSON string below
json = "{}"
# create an instance of JobStatus from a JSON string
job_status_instance = JobStatus.from_json(json)
# print the JSON string representation of the object
print JobStatus.to_json()

# convert the object into a dict
job_status_dict = job_status_instance.to_dict()
# create an instance of JobStatus from a dict
job_status_form_dict = job_status.from_dict(job_status_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


