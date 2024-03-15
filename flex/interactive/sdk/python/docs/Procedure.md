# Procedure


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**name** | **str** |  | [optional] 
**bound_graph** | **str** |  | [optional] 
**description** | **str** |  | [optional] 
**type** | **str** |  | [optional] 
**query** | **str** |  | [optional] 
**enable** | **bool** |  | [optional] 
**runnable** | **bool** |  | [optional] 
**params** | [**List[ProcedureParamsInner]**](ProcedureParamsInner.md) |  | [optional] 
**returns** | [**List[ProcedureParamsInner]**](ProcedureParamsInner.md) |  | [optional] 

## Example

```python
from interactive_sdk.models.procedure import Procedure

# TODO update the JSON string below
json = "{}"
# create an instance of Procedure from a JSON string
procedure_instance = Procedure.from_json(json)
# print the JSON string representation of the object
print Procedure.to_json()

# convert the object into a dict
procedure_dict = procedure_instance.to_dict()
# create an instance of Procedure from a dict
procedure_form_dict = procedure.from_dict(procedure_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


