# SnapshotStatus


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**snapshot_id** | **int** |  | 
**status** | **str** |  | 

## Example

```python
from gs_interactive.models.snapshot_status import SnapshotStatus

# TODO update the JSON string below
json = "{}"
# create an instance of SnapshotStatus from a JSON string
snapshot_status_instance = SnapshotStatus.from_json(json)
# print the JSON string representation of the object
print SnapshotStatus.to_json()

# convert the object into a dict
snapshot_status_dict = snapshot_status_instance.to_dict()
# create an instance of SnapshotStatus from a dict
snapshot_status_form_dict = snapshot_status.from_dict(snapshot_status_dict)
```
[[Back to Model list]](python_sdk.md#documentation-for-data-structures) [[Back to API list]](python_sdk.md#documentation-for-service-apis) [[Back to python_sdk]](python_sdk.md)


