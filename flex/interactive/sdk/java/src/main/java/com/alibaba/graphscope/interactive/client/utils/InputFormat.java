package com.alibaba.graphscope.interactive.client.utils;

/**
 * Enum representing various input formats for the Interactive SDK to submit queries to the Query service.
 */
public enum InputFormat {
    CPP_ENCODER, // Represents raw bytes serialized/deserialized via Encoder/Decoder.
    CYPHER_JSON, //  Indicates a JSON string adhering to the QueryRequest schema.
    CYPHER_PROTO_ADHOC, // Refers to the format for adhoc queries, presented as raw bytes serialized
    // from
    // proto
    // https://github.com/alibaba/GraphScope/blob/main/interactive_engine/executor/ir/proto/physical.proto
    CYPHER_PROTO_PROCEDURE, // Refers to the format for stored procedure queries, presented as raw
    // bytes
    // serialized from proto
    // https://github.com/alibaba/GraphScope/blob/main/interactive_engine/executor/ir/proto/stored_procedure.proto
}
