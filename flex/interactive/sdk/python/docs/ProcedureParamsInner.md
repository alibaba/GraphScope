# ProcedureParamsInner


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**name** | **str** |  | [optional] 
**type** | **str** |  | [optional] 

## Example

```python
from interactive_sdk.models.procedure_params_inner import ProcedureParamsInner

# TODO update the JSON string below
json = "{}"
# create an instance of ProcedureParamsInner from a JSON string
procedure_params_inner_instance = ProcedureParamsInner.from_json(json)
# print the JSON string representation of the object
print ProcedureParamsInner.to_json()

# convert the object into a dict
procedure_params_inner_dict = procedure_params_inner_instance.to_dict()
# create an instance of ProcedureParamsInner from a dict
procedure_params_inner_form_dict = procedure_params_inner.from_dict(procedure_params_inner_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


