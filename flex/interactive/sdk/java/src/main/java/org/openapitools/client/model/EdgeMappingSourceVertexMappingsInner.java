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
 * Mapping column to the primary key of source vertex
 */
@javax.annotation.Generated(value = "org.openapitools.codegen.languages.JavaClientCodegen")
public class EdgeMappingSourceVertexMappingsInner {
    public static final String SERIALIZED_NAME_COLUMN = "column";

    @SerializedName(SERIALIZED_NAME_COLUMN)
    private EdgeMappingSourceVertexMappingsInnerColumn column;

    public static final String SERIALIZED_NAME_PROPERTY = "property";

    @SerializedName(SERIALIZED_NAME_PROPERTY)
    private String property;

    public EdgeMappingSourceVertexMappingsInner() {}

    public EdgeMappingSourceVertexMappingsInner column(
            EdgeMappingSourceVertexMappingsInnerColumn column) {
        this.column = column;
        return this;
    }

    /**
     * Get column
     * @return column
     **/
    @javax.annotation.Nullable
    public EdgeMappingSourceVertexMappingsInnerColumn getColumn() {
        return column;
    }

    public void setColumn(EdgeMappingSourceVertexMappingsInnerColumn column) {
        this.column = column;
    }

    public EdgeMappingSourceVertexMappingsInner property(String property) {
        this.property = property;
        return this;
    }

    /**
     * Get property
     * @return property
     **/
    @javax.annotation.Nullable
    public String getProperty() {
        return property;
    }

    public void setProperty(String property) {
        this.property = property;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        EdgeMappingSourceVertexMappingsInner edgeMappingSourceVertexMappingsInner =
                (EdgeMappingSourceVertexMappingsInner) o;
        return Objects.equals(this.column, edgeMappingSourceVertexMappingsInner.column)
                && Objects.equals(this.property, edgeMappingSourceVertexMappingsInner.property);
    }

    @Override
    public int hashCode() {
        return Objects.hash(column, property);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("class EdgeMappingSourceVertexMappingsInner {\n");
        sb.append("    column: ").append(toIndentedString(column)).append("\n");
        sb.append("    property: ").append(toIndentedString(property)).append("\n");
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
        openapiFields.add("column");
        openapiFields.add("property");

        // a set of required properties/fields (JSON key names)
        openapiRequiredFields = new HashSet<String>();
    }

    /**
     * Validates the JSON Element and throws an exception if issues found
     *
     * @param jsonElement JSON Element
     * @throws IOException if the JSON Element is invalid with respect to EdgeMappingSourceVertexMappingsInner
     */
    public static void validateJsonElement(JsonElement jsonElement) throws IOException {
        if (jsonElement == null) {
            if (!EdgeMappingSourceVertexMappingsInner.openapiRequiredFields
                    .isEmpty()) { // has required fields but JSON element is null
                throw new IllegalArgumentException(
                        String.format(
                                "The required field(s) %s in EdgeMappingSourceVertexMappingsInner"
                                        + " is not found in the empty JSON string",
                                EdgeMappingSourceVertexMappingsInner.openapiRequiredFields
                                        .toString()));
            }
        }

        Set<Map.Entry<String, JsonElement>> entries = jsonElement.getAsJsonObject().entrySet();
        // check to see if the JSON string contains additional fields
        for (Map.Entry<String, JsonElement> entry : entries) {
            if (!EdgeMappingSourceVertexMappingsInner.openapiFields.contains(entry.getKey())) {
                throw new IllegalArgumentException(
                        String.format(
                                "The field `%s` in the JSON string is not defined in the"
                                    + " `EdgeMappingSourceVertexMappingsInner` properties. JSON:"
                                    + " %s",
                                entry.getKey(), jsonElement.toString()));
            }
        }
        JsonObject jsonObj = jsonElement.getAsJsonObject();
        // validate the optional field `column`
        if (jsonObj.get("column") != null && !jsonObj.get("column").isJsonNull()) {
            EdgeMappingSourceVertexMappingsInnerColumn.validateJsonElement(jsonObj.get("column"));
        }
        if ((jsonObj.get("property") != null && !jsonObj.get("property").isJsonNull())
                && !jsonObj.get("property").isJsonPrimitive()) {
            throw new IllegalArgumentException(
                    String.format(
                            "Expected the field `property` to be a primitive type in the JSON"
                                    + " string but got `%s`",
                            jsonObj.get("property").toString()));
        }
    }

    public static class CustomTypeAdapterFactory implements TypeAdapterFactory {
        @SuppressWarnings("unchecked")
        @Override
        public <T> TypeAdapter<T> create(Gson gson, TypeToken<T> type) {
            if (!EdgeMappingSourceVertexMappingsInner.class.isAssignableFrom(type.getRawType())) {
                return null; // this class only serializes 'EdgeMappingSourceVertexMappingsInner'
                // and its subtypes
            }
            final TypeAdapter<JsonElement> elementAdapter = gson.getAdapter(JsonElement.class);
            final TypeAdapter<EdgeMappingSourceVertexMappingsInner> thisAdapter =
                    gson.getDelegateAdapter(
                            this, TypeToken.get(EdgeMappingSourceVertexMappingsInner.class));

            return (TypeAdapter<T>)
                    new TypeAdapter<EdgeMappingSourceVertexMappingsInner>() {
                        @Override
                        public void write(
                                JsonWriter out, EdgeMappingSourceVertexMappingsInner value)
                                throws IOException {
                            JsonObject obj = thisAdapter.toJsonTree(value).getAsJsonObject();
                            elementAdapter.write(out, obj);
                        }

                        @Override
                        public EdgeMappingSourceVertexMappingsInner read(JsonReader in)
                                throws IOException {
                            JsonElement jsonElement = elementAdapter.read(in);
                            validateJsonElement(jsonElement);
                            return thisAdapter.fromJsonTree(jsonElement);
                        }
                    }.nullSafe();
        }
    }

    /**
     * Create an instance of EdgeMappingSourceVertexMappingsInner given an JSON string
     *
     * @param jsonString JSON string
     * @return An instance of EdgeMappingSourceVertexMappingsInner
     * @throws IOException if the JSON string is invalid with respect to EdgeMappingSourceVertexMappingsInner
     */
    public static EdgeMappingSourceVertexMappingsInner fromJson(String jsonString)
            throws IOException {
        return JSON.getGson().fromJson(jsonString, EdgeMappingSourceVertexMappingsInner.class);
    }

    /**
     * Convert an instance of EdgeMappingSourceVertexMappingsInner to an JSON string
     *
     * @return JSON string
     */
    public String toJson() {
        return JSON.getGson().toJson(this);
    }
}
