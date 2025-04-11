# ServiceRegistryRecord


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**service_name** | **str** |  | [optional] 
**instances** | [**List[RegistryInstanceRecord]**](RegistryInstanceRecord.md) |  | [optional] 
**primary** | [**RegistryInstanceRecord**](RegistryInstanceRecord.md) |  | [optional] 

## Example

```python
from gs_interactive.models.service_registry_record import ServiceRegistryRecord

# TODO update the JSON string below
json = "{}"
# create an instance of ServiceRegistryRecord from a JSON string
service_registry_record_instance = ServiceRegistryRecord.from_json(json)
# print the JSON string representation of the object
print ServiceRegistryRecord.to_json()

# convert the object into a dict
service_registry_record_dict = service_registry_record_instance.to_dict()
# create an instance of ServiceRegistryRecord from a dict
service_registry_record_form_dict = service_registry_record.from_dict(service_registry_record_dict)
```
[[Back to Model list]](python_sdk.md#documentation-for-data-structures) [[Back to API list]](python_sdk.md#documentation-for-service-apis) [[Back to python_sdk]](python_sdk.md)


