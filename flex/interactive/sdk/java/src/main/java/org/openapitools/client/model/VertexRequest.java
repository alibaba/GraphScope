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
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * VertexRequest
 */
@javax.annotation.Generated(value = "org.openapitools.codegen.languages.JavaClientCodegen")
public class VertexRequest {
    public static final String SERIALIZED_NAME_LABEL = "label";

    @SerializedName(SERIALIZED_NAME_LABEL)
    private String label;

    public static final String SERIALIZED_NAME_PRIMARY_KEY_VALUE = "primary_key_value";

    @SerializedName(SERIALIZED_NAME_PRIMARY_KEY_VALUE)
    private Object primaryKeyValue = null;

    public static final String SERIALIZED_NAME_PROPERTIES = "properties";

    @SerializedName(SERIALIZED_NAME_PROPERTIES)
    private PropertyArray properties;

    public VertexRequest() {}

    public VertexRequest label(String label) {
        this.label = label;
        return this;
    }

    /**
     * Get label
     * @return label
     **/
    @javax.annotation.Nonnull
    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public VertexRequest primaryKeyValue(Object primaryKeyValue) {
        this.primaryKeyValue = primaryKeyValue;
        return this;
    }

    /**
     * Get primaryKeyValue
     * @return primaryKeyValue
     **/
    @javax.annotation.Nullable
    public Object getPrimaryKeyValue() {
        return primaryKeyValue;
    }

    public void setPrimaryKeyValue(Object primaryKeyValue) {
        this.primaryKeyValue = primaryKeyValue;
    }

    public VertexRequest properties(PropertyArray properties) {
        this.properties = properties;
        return this;
    }

    /**
     * Get properties
     * @return properties
     **/
    @javax.annotation.Nullable
    public PropertyArray getProperties() {
        return properties;
    }

    public void setProperties(PropertyArray properties) {
        this.properties = properties;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        VertexRequest vertexRequest = (VertexRequest) o;
        return Objects.equals(this.label, vertexRequest.label)
                && Objects.equals(this.primaryKeyValue, vertexRequest.primaryKeyValue)
                && Objects.equals(this.properties, vertexRequest.properties);
    }

    @Override
    public int hashCode() {
        return Objects.hash(label, primaryKeyValue, properties);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("class VertexRequest {\n");
        sb.append("    label: ").append(toIndentedString(label)).append("\n");
        sb.append("    primaryKeyValue: ").append(toIndentedString(primaryKeyValue)).append("\n");
        sb.append("    properties: ").append(toIndentedString(properties)).append("\n");
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
        openapiFields.add("label");
        openapiFields.add("primary_key_value");
        openapiFields.add("properties");

        // a set of required properties/fields (JSON key names)
        openapiRequiredFields = new HashSet<String>();
        openapiRequiredFields.add("label");
        openapiRequiredFields.add("primary_key_value");
    }

    /**
     * Validates the JSON Element and throws an exception if issues found
     *
     * @param jsonElement JSON Element
     * @throws IOException if the JSON Element is invalid with respect to VertexRequest
     */
    public static void validateJsonElement(JsonElement jsonElement) throws IOException {
        if (jsonElement == null) {
            if (!VertexRequest.openapiRequiredFields
                    .isEmpty()) { // has required fields but JSON element is null
                throw new IllegalArgumentException(
                        String.format(
                                "The required field(s) %s in VertexRequest is not found in the"
                                        + " empty JSON string",
                                VertexRequest.openapiRequiredFields.toString()));
            }
        }

        Set<Map.Entry<String, JsonElement>> entries = jsonElement.getAsJsonObject().entrySet();
        // check to see if the JSON string contains additional fields
        for (Map.Entry<String, JsonElement> entry : entries) {
            if (!VertexRequest.openapiFields.contains(entry.getKey())) {
                throw new IllegalArgumentException(
                        String.format(
                                "The field `%s` in the JSON string is not defined in the"
                                        + " `VertexRequest` properties. JSON: %s",
                                entry.getKey(), jsonElement.toString()));
            }
        }

        // check to make sure all required properties/fields are present in the JSON string
        for (String requiredField : VertexRequest.openapiRequiredFields) {
            if (jsonElement.getAsJsonObject().get(requiredField) == null) {
                throw new IllegalArgumentException(
                        String.format(
                                "The required field `%s` is not found in the JSON string: %s",
                                requiredField, jsonElement.toString()));
            }
        }
        JsonObject jsonObj = jsonElement.getAsJsonObject();
        if (!jsonObj.get("label").isJsonPrimitive()) {
            throw new IllegalArgumentException(
                    String.format(
                            "Expected the field `label` to be a primitive type in the JSON string"
                                    + " but got `%s`",
                            jsonObj.get("label").toString()));
        }
        // validate the optional field `properties`
        if (jsonObj.get("properties") != null && !jsonObj.get("properties").isJsonNull()) {
            PropertyArray.validateJsonElement(jsonObj.get("properties"));
        }
    }

    public static class CustomTypeAdapterFactory implements TypeAdapterFactory {
        @SuppressWarnings("unchecked")
        @Override
        public <T> TypeAdapter<T> create(Gson gson, TypeToken<T> type) {
            if (!VertexRequest.class.isAssignableFrom(type.getRawType())) {
                return null; // this class only serializes 'VertexRequest' and its subtypes
            }
            final TypeAdapter<JsonElement> elementAdapter = gson.getAdapter(JsonElement.class);
            final TypeAdapter<VertexRequest> thisAdapter =
                    gson.getDelegateAdapter(this, TypeToken.get(VertexRequest.class));

            return (TypeAdapter<T>)
                    new TypeAdapter<VertexRequest>() {
                        @Override
                        public void write(JsonWriter out, VertexRequest value) throws IOException {
                            JsonObject obj = thisAdapter.toJsonTree(value).getAsJsonObject();
                            elementAdapter.write(out, obj);
                        }

                        @Override
                        public VertexRequest read(JsonReader in) throws IOException {
                            JsonElement jsonElement = elementAdapter.read(in);
                            validateJsonElement(jsonElement);
                            return thisAdapter.fromJsonTree(jsonElement);
                        }
                    }.nullSafe();
        }
    }

    /**
     * Create an instance of VertexRequest given an JSON string
     *
     * @param jsonString JSON string
     * @return An instance of VertexRequest
     * @throws IOException if the JSON string is invalid with respect to VertexRequest
     */
    public static VertexRequest fromJson(String jsonString) throws IOException {
        return JSON.getGson().fromJson(jsonString, VertexRequest.class);
    }

    /**
     * Convert an instance of VertexRequest to an JSON string
     *
     * @return JSON string
     */
    public String toJson() {
        return JSON.getGson().toJson(this);
    }
}
