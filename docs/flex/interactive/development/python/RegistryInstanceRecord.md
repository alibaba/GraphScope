# RegistryInstanceRecord


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**endpoint** | **str** |  | [optional] 
**metrics** | [**ServiceMetrics**](ServiceMetrics.md) |  | [optional] 

## Example

```python
from gs_interactive.models.registry_instance_record import RegistryInstanceRecord

# TODO update the JSON string below
json = "{}"
# create an instance of RegistryInstanceRecord from a JSON string
registry_instance_record_instance = RegistryInstanceRecord.from_json(json)
# print the JSON string representation of the object
print RegistryInstanceRecord.to_json()

# convert the object into a dict
registry_instance_record_dict = registry_instance_record_instance.to_dict()
# create an instance of RegistryInstanceRecord from a dict
registry_instance_record_form_dict = registry_instance_record.from_dict(registry_instance_record_dict)
```
[[Back to Model list]](python_sdk.md#documentation-for-data-structures) [[Back to API list]](python_sdk.md#documentation-for-service-apis) [[Back to python_sdk]](python_sdk.md)


