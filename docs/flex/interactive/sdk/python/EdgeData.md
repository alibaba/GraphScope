# EdgeData


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**src_label** | **str** |  | 
**dst_label** | **str** |  | 
**edge_label** | **str** |  | 
**src_primary_key_value** | **object** |  | 
**dst_primary_key_value** | **object** |  | 
**properties** | [**List[ModelProperty]**](ModelProperty.md) |  | 

## Example

```python
from interactive_sdk.models.edge_data import EdgeData

# TODO update the JSON string below
json = "{}"
# create an instance of EdgeData from a JSON string
edge_data_instance = EdgeData.from_json(json)
# print the JSON string representation of the object
print EdgeData.to_json()

# convert the object into a dict
edge_data_dict = edge_data_instance.to_dict()
# create an instance of EdgeData from a dict
edge_data_form_dict = edge_data.from_dict(edge_data_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


