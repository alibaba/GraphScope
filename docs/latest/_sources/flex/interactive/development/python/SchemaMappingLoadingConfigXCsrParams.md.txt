# SchemaMappingLoadingConfigXCsrParams

mutable_csr specific parameters

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**parallelism** | **int** |  | [optional] 
**build_csr_in_mem** | **bool** |  | [optional] 
**use_mmap_vector** | **bool** |  | [optional] 

## Example

```python
from gs_interactive.models.schema_mapping_loading_config_x_csr_params import SchemaMappingLoadingConfigXCsrParams

# TODO update the JSON string below
json = "{}"
# create an instance of SchemaMappingLoadingConfigXCsrParams from a JSON string
schema_mapping_loading_config_x_csr_params_instance = SchemaMappingLoadingConfigXCsrParams.from_json(json)
# print the JSON string representation of the object
print(SchemaMappingLoadingConfigXCsrParams.to_json())

# convert the object into a dict
schema_mapping_loading_config_x_csr_params_dict = schema_mapping_loading_config_x_csr_params_instance.to_dict()
# create an instance of SchemaMappingLoadingConfigXCsrParams from a dict
schema_mapping_loading_config_x_csr_params_from_dict = SchemaMappingLoadingConfigXCsrParams.from_dict(schema_mapping_loading_config_x_csr_params_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


