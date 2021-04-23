package com.alibaba.maxgraph.v2.frontend.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Client relate tool methods
 */
public class ClientUtil {
    private static final Logger logger = LoggerFactory.getLogger(ClientUtil.class);

    private static final String ARGS_QUERY_ID = "query.id";
    private static final String ARGS_QUEUE_NAME_LIST = "query.queue.name.list";
    private static final String ARGS_QUERY_SUBMIT_TIME = "query.submit.time";

    public static String getQueryId(Map<String, Object> args, Map<String, Object> argsBindings, String script) {
        String queryId = args.containsKey(ARGS_QUERY_ID) ?
                args.get(ARGS_QUERY_ID).toString() : (null != argsBindings && argsBindings.containsKey(ARGS_QUERY_ID) ?
                argsBindings.get(ARGS_QUERY_ID).toString() : null);
        if (null == queryId) {
            queryId = String.valueOf(ThreadLocalRandom.current().nextLong());
        } else {
            logger.info("Use args query id " + queryId + " for query " + script);
        }
        return queryId;
    }

    public static List<String> getQueueNameList(Map<String, Object> args, Map<String, Object> argsBindings) {
        return (List<String>) (args.containsKey(ARGS_QUEUE_NAME_LIST) ?
                args.get(ARGS_QUEUE_NAME_LIST) : (null != argsBindings && argsBindings.containsKey(ARGS_QUEUE_NAME_LIST) ?
                argsBindings.get(ARGS_QUEUE_NAME_LIST) : null));
    }

    public static long getQuerySubmitTime(Map<String, Object> args, Map<String, Object> argsBindings, String script) {
        if (args.containsKey(ARGS_QUERY_SUBMIT_TIME)) {
            return Long.parseLong(args.get(ARGS_QUERY_SUBMIT_TIME).toString());
        } else if (null != argsBindings && argsBindings.containsKey(ARGS_QUERY_SUBMIT_TIME)) {
            return Long.parseLong(argsBindings.get(ARGS_QUERY_SUBMIT_TIME).toString());
        } else {
            return System.currentTimeMillis();
        }
    }
}
