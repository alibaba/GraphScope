/*
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphscope.cypher.result;

import com.alibaba.graphscope.common.client.type.ExecutionResponseListener;
import com.alibaba.graphscope.common.config.QueryTimeoutConfig;
import com.alibaba.graphscope.common.result.RecordParser;
import com.alibaba.graphscope.common.utils.ClassUtils;
import com.alibaba.graphscope.gaia.proto.IrResult;
import com.alibaba.graphscope.proto.Code;
import com.alibaba.pegasus.common.StreamIterator;

import io.grpc.Status;

import org.neo4j.fabric.stream.summary.EmptySummary;
import org.neo4j.fabric.stream.summary.Summary;
import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.Notification;
import org.neo4j.graphdb.QueryExecutionType;
import org.neo4j.graphdb.QueryStatistics;
import org.neo4j.kernel.impl.query.QueryExecution;
import org.neo4j.kernel.impl.query.QuerySubscriber;
import org.neo4j.values.AnyValue;

import java.util.List;

/**
 * return streaming records in a reactive way
 */
public class CypherRecordProcessor implements QueryExecution, ExecutionResponseListener {
    private final RecordParser<AnyValue> recordParser;
    private final QuerySubscriber subscriber;
    private final StreamIterator<IrResult.Record> recordIterator;
    private final Summary summary;
    private final QueryTimeoutConfig timeoutConfig;

    public CypherRecordProcessor(
            RecordParser<AnyValue> recordParser,
            QuerySubscriber subscriber,
            QueryTimeoutConfig timeoutConfig) {
        this.recordParser = recordParser;
        this.subscriber = subscriber;
        this.recordIterator = new StreamIterator<>();
        this.summary = new EmptySummary();
        this.timeoutConfig = timeoutConfig;
        initializeSubscriber();
    }

    private void initializeSubscriber() {
        try {
            subscriber.onResult(fieldNames().length);
            subscriber.onRecord();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public QueryExecutionType executionType() {
        return QueryExecutionType.query(QueryExecutionType.QueryType.READ_ONLY);
    }

    @Override
    public ExecutionPlanDescription executionPlanDescription() {
        return this.summary.executionPlanDescription();
    }

    @Override
    public Iterable<Notification> getNotifications() {
        return this.summary.getNotifications();
    }

    @Override
    public String[] fieldNames() {
        return this.recordParser.schema().getFieldNames().toArray(new String[0]);
    }

    @Override
    public void request(long l) throws Exception {
        while (l > 0 && recordIterator.hasNext()) {
            IrResult.Record record = recordIterator.next();
            List<AnyValue> columns =
                    ClassUtils.callWithException(
                            () -> recordParser.parseFrom(record), Code.CYPHER_INVALID_RESULT);
            for (int i = 0; i < columns.size(); i++) {
                subscriber.onField(i, columns.get(i));
            }
            subscriber.onRecordCompleted();
            l--;
        }
        if (!recordIterator.hasNext()) {
            subscriber.onResultCompleted(QueryStatistics.EMPTY);
        }
    }

    @Override
    public void cancel() {
        this.recordIterator.close();
    }

    @Override
    public boolean await() throws Exception {
        return this.recordIterator.hasNext();
    }

    @Override
    public void onNext(IrResult.Record record) {
        try {
            this.recordIterator.putData(record);
        } catch (InterruptedException e) {
            onError(e);
        }
    }

    @Override
    public void onCompleted() {
        try {
            this.recordIterator.finish();
        } catch (InterruptedException e) {
            onError(e);
        }
    }

    @Override
    public void onError(Throwable t) {
        Status status =
                (t == null)
                        ? Status.UNKNOWN.withDescription("Unknown error in execution")
                        : Status.fromThrowable(t);
        this.recordIterator.fail(
                ClassUtils.handleExecutionException(status, timeoutConfig, t.getMessage()));
    }
}
