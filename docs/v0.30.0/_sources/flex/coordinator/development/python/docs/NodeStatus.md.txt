# NodeStatus


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**name** | **str** |  | 
**cpu_usage** | **float** |  | 
**memory_usage** | **float** |  | 
**disk_usage** | **float** |  | 

## Example

```python
from graphscope.flex.rest.models.node_status import NodeStatus

# TODO update the JSON string below
json = "{}"
# create an instance of NodeStatus from a JSON string
node_status_instance = NodeStatus.from_json(json)
# print the JSON string representation of the object
print(NodeStatus.to_json())

# convert the object into a dict
node_status_dict = node_status_instance.to_dict()
# create an instance of NodeStatus from a dict
node_status_from_dict = NodeStatus.from_dict(node_status_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


