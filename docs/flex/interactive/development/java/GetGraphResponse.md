

# GetGraphResponse


## Properties

| Name | Type | Description | Notes |
|------------ | ------------- | ------------- | -------------|
|**id** | **String** |  |  [optional] |
|**name** | **String** |  |  [optional] |
|**description** | **String** |  |  [optional] |
|**storeType** | [**StoreTypeEnum**](#StoreTypeEnum) |  |  [optional] |
|**creationTime** | **Integer** |  |  [optional] |
|**dataUpdateTime** | **Integer** |  |  [optional] |
|**storedProcedures** | [**List&lt;GetProcedureResponse&gt;**](GetProcedureResponse.md) |  |  [optional] |
|**schema** | [**GetGraphSchemaResponse**](GetGraphSchemaResponse.md) |  |  [optional] |
|**dataImportConfig** | [**SchemaMapping**](SchemaMapping.md) |  |  [optional] |



## Enum: StoreTypeEnum

| Name | Value |
|---- | -----|
| MUTABLE_CSR | &quot;mutable_csr&quot; |



