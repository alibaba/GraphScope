# StoredProcedureMeta


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**name** | **str** |  | 
**description** | **str** |  | [optional] 
**type** | **str** |  | 
**query** | **str** |  | 
**id** | **str** |  | [optional] 
**library** | **str** |  | [optional] 
**params** | [**List[Parameter]**](Parameter.md) |  | [optional] 
**returns** | [**List[Parameter]**](Parameter.md) |  | [optional] 
**enable** | **bool** |  | [optional] 

## Example

```python
from interactive_sdk.models.stored_procedure_meta import StoredProcedureMeta

# TODO update the JSON string below
json = "{}"
# create an instance of StoredProcedureMeta from a JSON string
stored_procedure_meta_instance = StoredProcedureMeta.from_json(json)
# print the JSON string representation of the object
print StoredProcedureMeta.to_json()

# convert the object into a dict
stored_procedure_meta_dict = stored_procedure_meta_instance.to_dict()
# create an instance of StoredProcedureMeta from a dict
stored_procedure_meta_form_dict = stored_procedure_meta.from_dict(stored_procedure_meta_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


