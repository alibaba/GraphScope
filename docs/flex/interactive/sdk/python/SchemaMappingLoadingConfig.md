# SchemaMappingLoadingConfig


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**import_option** | **str** |  | [optional] 
**format** | [**SchemaMappingLoadingConfigFormat**](SchemaMappingLoadingConfigFormat.md) |  | [optional] 

## Example

```python
from interactive_sdk.models.schema_mapping_loading_config import SchemaMappingLoadingConfig

# TODO update the JSON string below
json = "{}"
# create an instance of SchemaMappingLoadingConfig from a JSON string
schema_mapping_loading_config_instance = SchemaMappingLoadingConfig.from_json(json)
# print the JSON string representation of the object
print SchemaMappingLoadingConfig.to_json()

# convert the object into a dict
schema_mapping_loading_config_dict = schema_mapping_loading_config_instance.to_dict()
# create an instance of SchemaMappingLoadingConfig from a dict
schema_mapping_loading_config_form_dict = schema_mapping_loading_config.from_dict(schema_mapping_loading_config_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


