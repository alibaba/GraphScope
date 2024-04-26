/*
 * GraphScope Interactive API v0.0.3
 * This is the definition of GraphScope Interactive API, including   - AdminService API   - Vertex/Edge API   - QueryService   AdminService API (with tag AdminService) defines the API for GraphManagement, ProcedureManagement and Service Management.  Vertex/Edge API (with tag GraphService) defines the API for Vertex/Edge management, including creation/updating/delete/retrive.  QueryService API (with tag QueryService) defines the API for procedure_call, Ahodc query.
 *
 * The version of the OpenAPI document: 1.0.0
 * Contact: graphscope@alibaba-inc.com
 *
 * NOTE: This class is auto generated by OpenAPI Generator (https://openapi-generator.tech).
 * https://openapi-generator.tech
 * Do not edit the class manually.
 */

package org.openapitools.client.model;

import com.alibaba.graphscope.JSON;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.TypeAdapter;
import com.google.gson.TypeAdapterFactory;
import com.google.gson.annotations.SerializedName;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * BaseVertexType
 */
@javax.annotation.Generated(value = "org.openapitools.codegen.languages.JavaClientCodegen")
public class BaseVertexType {
    public static final String SERIALIZED_NAME_TYPE_NAME = "type_name";

    @SerializedName(SERIALIZED_NAME_TYPE_NAME)
    private String typeName;

    public static final String SERIALIZED_NAME_PRIMARY_KEYS = "primary_keys";

    @SerializedName(SERIALIZED_NAME_PRIMARY_KEYS)
    private List<String> primaryKeys;

    public static final String SERIALIZED_NAME_X_CSR_PARAMS = "x_csr_params";

    @SerializedName(SERIALIZED_NAME_X_CSR_PARAMS)
    private BaseVertexTypeXCsrParams xCsrParams;

    public BaseVertexType() {}

    public BaseVertexType typeName(String typeName) {
        this.typeName = typeName;
        return this;
    }

    /**
     * Get typeName
     * @return typeName
     **/
    @javax.annotation.Nullable
    public String getTypeName() {
        return typeName;
    }

    public void setTypeName(String typeName) {
        this.typeName = typeName;
    }

    public BaseVertexType primaryKeys(List<String> primaryKeys) {
        this.primaryKeys = primaryKeys;
        return this;
    }

    public BaseVertexType addPrimaryKeysItem(String primaryKeysItem) {
        if (this.primaryKeys == null) {
            this.primaryKeys = new ArrayList<>();
        }
        this.primaryKeys.add(primaryKeysItem);
        return this;
    }

    /**
     * Get primaryKeys
     * @return primaryKeys
     **/
    @javax.annotation.Nullable
    public List<String> getPrimaryKeys() {
        return primaryKeys;
    }

    public void setPrimaryKeys(List<String> primaryKeys) {
        this.primaryKeys = primaryKeys;
    }

    public BaseVertexType xCsrParams(BaseVertexTypeXCsrParams xCsrParams) {
        this.xCsrParams = xCsrParams;
        return this;
    }

    /**
     * Get xCsrParams
     * @return xCsrParams
     **/
    @javax.annotation.Nullable
    public BaseVertexTypeXCsrParams getxCsrParams() {
        return xCsrParams;
    }

    public void setxCsrParams(BaseVertexTypeXCsrParams xCsrParams) {
        this.xCsrParams = xCsrParams;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BaseVertexType baseVertexType = (BaseVertexType) o;
        return Objects.equals(this.typeName, baseVertexType.typeName)
                && Objects.equals(this.primaryKeys, baseVertexType.primaryKeys)
                && Objects.equals(this.xCsrParams, baseVertexType.xCsrParams);
    }

    @Override
    public int hashCode() {
        return Objects.hash(typeName, primaryKeys, xCsrParams);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("class BaseVertexType {\n");
        sb.append("    typeName: ").append(toIndentedString(typeName)).append("\n");
        sb.append("    primaryKeys: ").append(toIndentedString(primaryKeys)).append("\n");
        sb.append("    xCsrParams: ").append(toIndentedString(xCsrParams)).append("\n");
        sb.append("}");
        return sb.toString();
    }

    /**
     * Convert the given object to string with each line indented by 4 spaces
     * (except the first line).
     */
    private String toIndentedString(Object o) {
        if (o == null) {
            return "null";
        }
        return o.toString().replace("\n", "\n    ");
    }

    public static HashSet<String> openapiFields;
    public static HashSet<String> openapiRequiredFields;

    static {
        // a set of all properties/fields (JSON key names)
        openapiFields = new HashSet<String>();
        openapiFields.add("type_name");
        openapiFields.add("primary_keys");
        openapiFields.add("x_csr_params");

        // a set of required properties/fields (JSON key names)
        openapiRequiredFields = new HashSet<String>();
    }

    /**
     * Validates the JSON Element and throws an exception if issues found
     *
     * @param jsonElement JSON Element
     * @throws IOException if the JSON Element is invalid with respect to BaseVertexType
     */
    public static void validateJsonElement(JsonElement jsonElement) throws IOException {
        if (jsonElement == null) {
            if (!BaseVertexType.openapiRequiredFields
                    .isEmpty()) { // has required fields but JSON element is null
                throw new IllegalArgumentException(
                        String.format(
                                "The required field(s) %s in BaseVertexType is not found in the"
                                        + " empty JSON string",
                                BaseVertexType.openapiRequiredFields.toString()));
            }
        }

        Set<Map.Entry<String, JsonElement>> entries = jsonElement.getAsJsonObject().entrySet();
        // check to see if the JSON string contains additional fields
        for (Map.Entry<String, JsonElement> entry : entries) {
            if (!BaseVertexType.openapiFields.contains(entry.getKey())) {
                throw new IllegalArgumentException(
                        String.format(
                                "The field `%s` in the JSON string is not defined in the"
                                        + " `BaseVertexType` properties. JSON: %s",
                                entry.getKey(), jsonElement.toString()));
            }
        }
        JsonObject jsonObj = jsonElement.getAsJsonObject();
        if ((jsonObj.get("type_name") != null && !jsonObj.get("type_name").isJsonNull())
                && !jsonObj.get("type_name").isJsonPrimitive()) {
            throw new IllegalArgumentException(
                    String.format(
                            "Expected the field `type_name` to be a primitive type in the JSON"
                                    + " string but got `%s`",
                            jsonObj.get("type_name").toString()));
        }
        // ensure the optional json data is an array if present
        if (jsonObj.get("primary_keys") != null
                && !jsonObj.get("primary_keys").isJsonNull()
                && !jsonObj.get("primary_keys").isJsonArray()) {
            throw new IllegalArgumentException(
                    String.format(
                            "Expected the field `primary_keys` to be an array in the JSON string"
                                    + " but got `%s`",
                            jsonObj.get("primary_keys").toString()));
        }
        // validate the optional field `x_csr_params`
        if (jsonObj.get("x_csr_params") != null && !jsonObj.get("x_csr_params").isJsonNull()) {
            BaseVertexTypeXCsrParams.validateJsonElement(jsonObj.get("x_csr_params"));
        }
    }

    public static class CustomTypeAdapterFactory implements TypeAdapterFactory {
        @SuppressWarnings("unchecked")
        @Override
        public <T> TypeAdapter<T> create(Gson gson, TypeToken<T> type) {
            if (!BaseVertexType.class.isAssignableFrom(type.getRawType())) {
                return null; // this class only serializes 'BaseVertexType' and its subtypes
            }
            final TypeAdapter<JsonElement> elementAdapter = gson.getAdapter(JsonElement.class);
            final TypeAdapter<BaseVertexType> thisAdapter =
                    gson.getDelegateAdapter(this, TypeToken.get(BaseVertexType.class));

            return (TypeAdapter<T>)
                    new TypeAdapter<BaseVertexType>() {
                        @Override
                        public void write(JsonWriter out, BaseVertexType value) throws IOException {
                            JsonObject obj = thisAdapter.toJsonTree(value).getAsJsonObject();
                            elementAdapter.write(out, obj);
                        }

                        @Override
                        public BaseVertexType read(JsonReader in) throws IOException {
                            JsonElement jsonElement = elementAdapter.read(in);
                            validateJsonElement(jsonElement);
                            return thisAdapter.fromJsonTree(jsonElement);
                        }
                    }.nullSafe();
        }
    }

    /**
     * Create an instance of BaseVertexType given an JSON string
     *
     * @param jsonString JSON string
     * @return An instance of BaseVertexType
     * @throws IOException if the JSON string is invalid with respect to BaseVertexType
     */
    public static BaseVertexType fromJson(String jsonString) throws IOException {
        return JSON.getGson().fromJson(jsonString, BaseVertexType.class);
    }

    /**
     * Convert an instance of BaseVertexType to an JSON string
     *
     * @return JSON string
     */
    public String toJson() {
        return JSON.getGson().toJson(this);
    }
}
