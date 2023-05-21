package com.alibaba.graphscope.groot.dataload.util;

public class Constants {

    // Get property

    /** universal configurations **/
    public static final String GRAPH_ENDPOINT = "graph.endpoint";

    public static final String COLUMN_MAPPING_CONFIG = "column.mapping.config";

    public static final String LOAD_AFTER_BUILD = "load.after.build";

    public static final String SPLIT_SIZE = "split.size";

    public static final String UNIQUE_PATH = "unique.path"; // generated automatically for each task
    public static final String USER_NAME = "auth.username";
    public static final String PASS_WORD = "auth.password";

    /** job on HDFS configurations **/

    // Input and output
    public static final String INPUT_PATH = "input.path";

    public static final String OUTPUT_PATH = "output.path";
    public static final String SEPARATOR = "separator";
    public static final String SKIP_HEADER = "skip.header";
    public static final String LDBC_CUSTOMIZE = "ldbc.customize";
    /* end */

    /** job on ODPS configurations **/
    public static final String DATA_SINK_TYPE = "data.sink.type"; // hdfs, oss, volume
    // The table format is `project.table` or `table`;
    // For partitioned table, the format is `project.table|p1=1/p2=2` or `table|p1=1/p2=2`
    public static final String OUTPUT_TABLE = "output.table"; // a dummy table
    /* end */

    // Set property
    public static final String SCHEMA_JSON = "schema.json";
    public static final String COLUMN_MAPPINGS = "column.mappings";
    public static final String META_INFO = "meta.info";

    public static final String META_FILE_NAME = "META";

    /** OSS configurations **/
    public static final String OSS_ENDPOINT = "oss.endpoint";

    public static final String OSS_ACCESS_ID = "oss.access.id";
    public static final String OSS_ACCESS_KEY = "oss.access.key";

    public static final String OSS_BUCKET_NAME = "oss.bucket.name";
    public static final String OSS_OBJECT_NAME = "oss.object.name";
    public static final String OSS_INFO_URL = "oss.info.url";
    /* end */

    /** ODPS Volume configurations **/
    public static final String ODPS_VOLUME_PROJECT = "odps.volume.project";

    public static final String ODPS_VOLUME_NAME = "odps.volume.name";
    public static final String ODPS_VOLUME_PARTSPEC = "odps.volume.partspec";

    public static final String ODPS_ACCESS_ID = "odps.access.id";
    public static final String ODPS_ACCESS_KEY = "odps.access.key";
    public static final String ODPS_ENDPOINT = "odps.endpoint";
    /* end */

}
