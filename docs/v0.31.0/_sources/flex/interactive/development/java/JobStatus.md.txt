

# JobStatus


## Properties

| Name | Type | Description | Notes |
|------------ | ------------- | ------------- | -------------|
|**id** | **String** |  |  [optional] |
|**type** | **String** |  |  [optional] |
|**status** | [**StatusEnum**](#StatusEnum) |  |  [optional] |
|**startTime** | **Integer** |  |  [optional] |
|**endTime** | **Integer** |  |  [optional] |
|**log** | **String** | URL or log string |  [optional] |
|**detail** | **Map&lt;String, Object&gt;** |  |  [optional] |



## Enum: StatusEnum

| Name | Value |
|---- | -----|
| RUNNING | &quot;RUNNING&quot; |
| SUCCESS | &quot;SUCCESS&quot; |
| FAILED | &quot;FAILED&quot; |
| CANCELLED | &quot;CANCELLED&quot; |
| WAITING | &quot;WAITING&quot; |



