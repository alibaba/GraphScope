# SchemaMappingLoadingConfigFormat


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**type** | **str** |  | [optional] 
**metadata** | **Dict[str, object]** |  | [optional] 

## Example

```python
from interactive_sdk.models.schema_mapping_loading_config_format import SchemaMappingLoadingConfigFormat

# TODO update the JSON string below
json = "{}"
# create an instance of SchemaMappingLoadingConfigFormat from a JSON string
schema_mapping_loading_config_format_instance = SchemaMappingLoadingConfigFormat.from_json(json)
# print the JSON string representation of the object
print SchemaMappingLoadingConfigFormat.to_json()

# convert the object into a dict
schema_mapping_loading_config_format_dict = schema_mapping_loading_config_format_instance.to_dict()
# create an instance of SchemaMappingLoadingConfigFormat from a dict
schema_mapping_loading_config_format_form_dict = schema_mapping_loading_config_format.from_dict(schema_mapping_loading_config_format_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


