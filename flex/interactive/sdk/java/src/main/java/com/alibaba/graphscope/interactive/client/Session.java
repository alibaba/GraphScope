package com.alibaba.graphscope.interactive.client;

import com.alibaba.graphscope.interactive.client.common.Result;
import org.openapitools.client.model.*;

import java.io.Closeable;
import java.util.List;

public class Session implements VertexInterface,EdgeInterface,GraphInterface,JobInterface,ProcedureInterface,QueryServiceInterface, AutoCloseable{

    @Override
    public Result<EdgeData> getEdge(String graphName, String edgeLabel, String srcLabel, Object srcPrimaryKeyValue, String dstLabel, Object dstPrimaryKeyValue) {
        return null;
    }

    @Override
    public Result<String> addEdge(String graphName, EdgeRequest edgeRequest) {
        return null;
    }

    @Override
    public Result<String> deleteEdge(String graphName, String srcLabel, Object srcPrimaryKeyValue, String dstLabel, Object dstPrimaryKeyValue) {
        return null;
    }

    @Override
    public Result<String> updateEdge(String graphName, EdgeRequest edgeRequest) {
        return null;
    }

    @Override
    public Result<JobResponse> bulkLoading(String graphId, SchemaMapping mapping) {
        return null;
    }

    @Override
    public Result<String> createGraph(Graph graph) {
        return null;
    }

    @Override
    public Result<String> deleteGraph(String graphId) {
        return null;
    }

    @Override
    public Result<String> updateGraph(String graphId, Graph graph) {
        return null;
    }

    @Override
    public Result<GraphSchema> getGraphSchema(String graphId) {
        return null;
    }

    @Override
    public Result<Graph> getAllGraphs() {
        return null;
    }

    @Override
    public Result<String> cancelJob(String jobId) {
        return null;
    }

    @Override
    public Result<JobStatus> getJobStatus(String jobId) {
        return null;
    }

    @Override
    public Result<List<JobStatus>> listJobs() {
        return null;
    }

    @Override
    public Result<String> createProcedure(String graphId, Procedure procedure) {
        return null;
    }

    @Override
    public Result<String> deleteProcedure(String graphId, String procedureName) {
        return null;
    }

    @Override
    public Result<Procedure> getProcedure(String graphId, String procedureName) {
        return null;
    }

    @Override
    public Result<List<Procedure>> listProcedures(String graphId) {
        return null;
    }

    @Override
    public Result<String> updateProcedure(String graphId, String procedureId, Procedure procedure) {
        return null;
    }

    @Override
    public Result<String> callProcedure() {
        return null;
    }

    @Override
    public Result<ServiceStatus> getServiceStatus() {
        return null;
    }

    @Override
    public Result<String> restartService() {
        return null;
    }

    @Override
    public Result<String> startService() {
        return null;
    }

    @Override
    public Result<String> stopService() {
        return null;
    }

    @Override
    public Result<String> addVertex(String graphId, VertexRequest request) {
        return null;
    }

    @Override
    public Result<String> updateVertex(String graphId, VertexRequest request) {
        return null;
    }

    @Override
    public Result<VertexData> getVertex(String graphId, String label, Object primaryKey) {
        return null;
    }

    @Override
    public Result<String> deleteVertex(String graphId, String label, Object primaryKey) {
        return null;
    }

    /**
     * Closes this resource, relinquishing any underlying resources.
     * This method is invoked automatically on objects managed by the
     * {@code try}-with-resources statement.
     *
     * <p>While this interface method is declared to throw {@code
     * Exception}, implementers are <em>strongly</em> encouraged to
     * declare concrete implementations of the {@code close} method to
     * throw more specific exceptions, or to throw no exception at all
     * if the close operation cannot fail.
     *
     * <p> Cases where the close operation may fail require careful
     * attention by implementers. It is strongly advised to relinquish
     * the underlying resources and to internally <em>mark</em> the
     * resource as closed, prior to throwing the exception. The {@code
     * close} method is unlikely to be invoked more than once and so
     * this ensures that the resources are released in a timely manner.
     * Furthermore it reduces problems that could arise when the resource
     * wraps, or is wrapped, by another resource.
     *
     * <p><em>Implementers of this interface are also strongly advised
     * to not have the {@code close} method throw {@link
     * InterruptedException}.</em>
     * <p>
     * This exception interacts with a thread's interrupted status,
     * and runtime misbehavior is likely to occur if an {@code
     * InterruptedException} is {@linkplain Throwable#addSuppressed
     * suppressed}.
     * <p>
     * More generally, if it would cause problems for an
     * exception to be suppressed, the {@code AutoCloseable.close}
     * method should not throw it.
     *
     * <p>Note that unlike the {@link Closeable#close close}
     * method of {@link Closeable}, this {@code close} method
     * is <em>not</em> required to be idempotent.  In other words,
     * calling this {@code close} method more than once may have some
     * visible side effect, unlike {@code Closeable.close} which is
     * required to have no effect if called more than once.
     * <p>
     * However, implementers of this interface are strongly encouraged
     * to make their {@code close} methods idempotent.
     *
     * @throws Exception if this resource cannot be closed
     */
    @Override
    public void close() throws Exception {

    }
}
