# CreateProcedureRequest


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**name** | **str** |  | 
**description** | **str** |  | [optional] 
**type** | **str** |  | 
**query** | **str** |  | 

## Example

```python
from interactive_sdk.models.create_procedure_request import CreateProcedureRequest

# TODO update the JSON string below
json = "{}"
# create an instance of CreateProcedureRequest from a JSON string
create_procedure_request_instance = CreateProcedureRequest.from_json(json)
# print the JSON string representation of the object
print CreateProcedureRequest.to_json()

# convert the object into a dict
create_procedure_request_dict = create_procedure_request_instance.to_dict()
# create an instance of CreateProcedureRequest from a dict
create_procedure_request_form_dict = create_procedure_request.from_dict(create_procedure_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


