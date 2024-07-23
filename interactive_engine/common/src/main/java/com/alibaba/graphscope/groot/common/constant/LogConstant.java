package com.alibaba.graphscope.groot.common.constant;

public class LogConstant {

    public static String TRACE_ID = "traceId";

    public static String UPSTREAM_ID = "upstreamId";

    public static String QUERY_ID = "queryId";

    /**
     * 具体查询语句
     */
    public static String QUERY = "query";

    public static String SUCCESS = "success";

    public static String ERROR_MESSAGE = "errorMessage";

    public static String STACK_TRACE = "stackTrace";

    /**
     * 查询计划
     */
    public static String IR_PLAN = "irPlan";

    /**
     * 打印日志的阶段
     * query: java/rust
     * write: writeKafka/consumeKafka/writeDb
     */
    public static String STAGE = "stage";

    public static String RESULT = "result";

    public static String COST = "cost";

    public static String START_TIME = "startMillis";

    public static String END_TIME = "endMillis";

    /**
     * 日志类型: query/write
     */
    public static String LOG_TYPE = "logType";

    public static String BATCH_SIZE = "batchSize";

    public static String PARTITION_ID = "partitionId";
}
