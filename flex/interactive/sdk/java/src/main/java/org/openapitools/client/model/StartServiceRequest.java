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
 * StartServiceRequest
 */
@javax.annotation.Generated(value = "org.openapitools.codegen.languages.JavaClientCodegen")
public class StartServiceRequest {
    public static final String SERIALIZED_NAME_GRAPH_ID = "graph_id";

    @SerializedName(SERIALIZED_NAME_GRAPH_ID)
    private String graphId;

    public StartServiceRequest() {}

    public StartServiceRequest graphId(String graphId) {
        this.graphId = graphId;
        return this;
    }

    /**
     * Get graphId
     * @return graphId
     **/
    @javax.annotation.Nullable
    public String getGraphId() {
        return graphId;
    }

    public void setGraphId(String graphId) {
        this.graphId = graphId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        StartServiceRequest startServiceRequest = (StartServiceRequest) o;
        return Objects.equals(this.graphId, startServiceRequest.graphId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(graphId);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("class StartServiceRequest {\n");
        sb.append("    graphId: ").append(toIndentedString(graphId)).append("\n");
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
        openapiFields.add("graph_id");

        // a set of required properties/fields (JSON key names)
        openapiRequiredFields = new HashSet<String>();
    }

    /**
     * Validates the JSON Element and throws an exception if issues found
     *
     * @param jsonElement JSON Element
     * @throws IOException if the JSON Element is invalid with respect to StartServiceRequest
     */
    public static void validateJsonElement(JsonElement jsonElement) throws IOException {
        if (jsonElement == null) {
            if (!StartServiceRequest.openapiRequiredFields
                    .isEmpty()) { // has required fields but JSON element is null
                throw new IllegalArgumentException(
                        String.format(
                                "The required field(s) %s in StartServiceRequest is not found in"
                                        + " the empty JSON string",
                                StartServiceRequest.openapiRequiredFields.toString()));
            }
        }

        Set<Map.Entry<String, JsonElement>> entries = jsonElement.getAsJsonObject().entrySet();
        // check to see if the JSON string contains additional fields
        for (Map.Entry<String, JsonElement> entry : entries) {
            if (!StartServiceRequest.openapiFields.contains(entry.getKey())) {
                throw new IllegalArgumentException(
                        String.format(
                                "The field `%s` in the JSON string is not defined in the"
                                        + " `StartServiceRequest` properties. JSON: %s",
                                entry.getKey(), jsonElement.toString()));
            }
        }
        JsonObject jsonObj = jsonElement.getAsJsonObject();
        if ((jsonObj.get("graph_id") != null && !jsonObj.get("graph_id").isJsonNull())
                && !jsonObj.get("graph_id").isJsonPrimitive()) {
            throw new IllegalArgumentException(
                    String.format(
                            "Expected the field `graph_id` to be a primitive type in the JSON"
                                    + " string but got `%s`",
                            jsonObj.get("graph_id").toString()));
        }
    }

    public static class CustomTypeAdapterFactory implements TypeAdapterFactory {
        @SuppressWarnings("unchecked")
        @Override
        public <T> TypeAdapter<T> create(Gson gson, TypeToken<T> type) {
            if (!StartServiceRequest.class.isAssignableFrom(type.getRawType())) {
                return null; // this class only serializes 'StartServiceRequest' and its subtypes
            }
            final TypeAdapter<JsonElement> elementAdapter = gson.getAdapter(JsonElement.class);
            final TypeAdapter<StartServiceRequest> thisAdapter =
                    gson.getDelegateAdapter(this, TypeToken.get(StartServiceRequest.class));

            return (TypeAdapter<T>)
                    new TypeAdapter<StartServiceRequest>() {
                        @Override
                        public void write(JsonWriter out, StartServiceRequest value)
                                throws IOException {
                            JsonObject obj = thisAdapter.toJsonTree(value).getAsJsonObject();
                            elementAdapter.write(out, obj);
                        }

                        @Override
                        public StartServiceRequest read(JsonReader in) throws IOException {
                            JsonElement jsonElement = elementAdapter.read(in);
                            validateJsonElement(jsonElement);
                            return thisAdapter.fromJsonTree(jsonElement);
                        }
                    }.nullSafe();
        }
    }

    /**
     * Create an instance of StartServiceRequest given an JSON string
     *
     * @param jsonString JSON string
     * @return An instance of StartServiceRequest
     * @throws IOException if the JSON string is invalid with respect to StartServiceRequest
     */
    public static StartServiceRequest fromJson(String jsonString) throws IOException {
        return JSON.getGson().fromJson(jsonString, StartServiceRequest.class);
    }

    /**
     * Convert an instance of StartServiceRequest to an JSON string
     *
     * @return JSON string
     */
    public String toJson() {
        return JSON.getGson().toJson(this);
    }
}
