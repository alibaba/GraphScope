package com.alibaba.maxgraph.proto;

import static io.grpc.stub.ClientCalls.asyncUnaryCall;
import static io.grpc.stub.ClientCalls.asyncServerStreamingCall;
import static io.grpc.stub.ClientCalls.asyncClientStreamingCall;
import static io.grpc.stub.ClientCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ClientCalls.blockingUnaryCall;
import static io.grpc.stub.ClientCalls.blockingServerStreamingCall;
import static io.grpc.stub.ClientCalls.futureUnaryCall;
import static io.grpc.MethodDescriptor.generateFullMethodName;
import static io.grpc.stub.ServerCalls.asyncUnaryCall;
import static io.grpc.stub.ServerCalls.asyncServerStreamingCall;
import static io.grpc.stub.ServerCalls.asyncClientStreamingCall;
import static io.grpc.stub.ServerCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.0.0)",
    comments = "Source: debug.proto")
public class DebugServiceApiGrpc {

  private DebugServiceApiGrpc() {}

  public static final String SERVICE_NAME = "DebugServiceApi";

  // Static method descriptors that strictly reflect the proto.
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.alibaba.maxgraph.proto.Empty,
      com.alibaba.maxgraph.proto.ServerInfo> METHOD_GET_SERVER_INFO =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "DebugServiceApi", "getServerInfo"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.Empty.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.ServerInfo.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.alibaba.maxgraph.proto.Empty,
      com.alibaba.maxgraph.proto.GraphInfo> METHOD_GET_GRAPH_INFO =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "DebugServiceApi", "getGraphInfo"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.Empty.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.GraphInfo.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.alibaba.maxgraph.proto.GetVertexRequest,
      com.alibaba.maxgraph.proto.VertexProto> METHOD_GET_VERTEX =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING,
          generateFullMethodName(
              "DebugServiceApi", "getVertex"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.GetVertexRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.VertexProto.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.alibaba.maxgraph.proto.ScanVertexRequest,
      com.alibaba.maxgraph.proto.VertexProto> METHOD_SCAN_VERTEX =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING,
          generateFullMethodName(
              "DebugServiceApi", "scanVertex"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.ScanVertexRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.VertexProto.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.alibaba.maxgraph.proto.GetOutEdgesRequest,
      com.alibaba.maxgraph.proto.EdgeProto> METHOD_GET_OUT_EDGES =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING,
          generateFullMethodName(
              "DebugServiceApi", "getOutEdges"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.GetOutEdgesRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.EdgeProto.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.alibaba.maxgraph.proto.GetInEdgesRequest,
      com.alibaba.maxgraph.proto.EdgeProto> METHOD_GET_IN_EDGES =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING,
          generateFullMethodName(
              "DebugServiceApi", "getInEdges"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.GetInEdgesRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.EdgeProto.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.alibaba.maxgraph.proto.GetSchemaRequest,
      com.alibaba.maxgraph.proto.SchemaProto> METHOD_GET_SCHEMA =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "DebugServiceApi", "getSchema"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.GetSchemaRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.alibaba.maxgraph.proto.SchemaProto.getDefaultInstance()));

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static DebugServiceApiStub newStub(io.grpc.Channel channel) {
    return new DebugServiceApiStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static DebugServiceApiBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new DebugServiceApiBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary and streaming output calls on the service
   */
  public static DebugServiceApiFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new DebugServiceApiFutureStub(channel);
  }

  /**
   */
  public static abstract class DebugServiceApiImplBase implements io.grpc.BindableService {

    /**
     */
    public void getServerInfo(com.alibaba.maxgraph.proto.Empty request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.ServerInfo> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_GET_SERVER_INFO, responseObserver);
    }

    /**
     */
    public void getGraphInfo(com.alibaba.maxgraph.proto.Empty request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.GraphInfo> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_GET_GRAPH_INFO, responseObserver);
    }

    /**
     */
    public void getVertex(com.alibaba.maxgraph.proto.GetVertexRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.VertexProto> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_GET_VERTEX, responseObserver);
    }

    /**
     */
    public void scanVertex(com.alibaba.maxgraph.proto.ScanVertexRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.VertexProto> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_SCAN_VERTEX, responseObserver);
    }

    /**
     */
    public void getOutEdges(com.alibaba.maxgraph.proto.GetOutEdgesRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.EdgeProto> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_GET_OUT_EDGES, responseObserver);
    }

    /**
     */
    public void getInEdges(com.alibaba.maxgraph.proto.GetInEdgesRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.EdgeProto> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_GET_IN_EDGES, responseObserver);
    }

    /**
     */
    public void getSchema(com.alibaba.maxgraph.proto.GetSchemaRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.SchemaProto> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_GET_SCHEMA, responseObserver);
    }

    @java.lang.Override public io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            METHOD_GET_SERVER_INFO,
            asyncUnaryCall(
              new MethodHandlers<
                com.alibaba.maxgraph.proto.Empty,
                com.alibaba.maxgraph.proto.ServerInfo>(
                  this, METHODID_GET_SERVER_INFO)))
          .addMethod(
            METHOD_GET_GRAPH_INFO,
            asyncUnaryCall(
              new MethodHandlers<
                com.alibaba.maxgraph.proto.Empty,
                com.alibaba.maxgraph.proto.GraphInfo>(
                  this, METHODID_GET_GRAPH_INFO)))
          .addMethod(
            METHOD_GET_VERTEX,
            asyncServerStreamingCall(
              new MethodHandlers<
                com.alibaba.maxgraph.proto.GetVertexRequest,
                com.alibaba.maxgraph.proto.VertexProto>(
                  this, METHODID_GET_VERTEX)))
          .addMethod(
            METHOD_SCAN_VERTEX,
            asyncServerStreamingCall(
              new MethodHandlers<
                com.alibaba.maxgraph.proto.ScanVertexRequest,
                com.alibaba.maxgraph.proto.VertexProto>(
                  this, METHODID_SCAN_VERTEX)))
          .addMethod(
            METHOD_GET_OUT_EDGES,
            asyncServerStreamingCall(
              new MethodHandlers<
                com.alibaba.maxgraph.proto.GetOutEdgesRequest,
                com.alibaba.maxgraph.proto.EdgeProto>(
                  this, METHODID_GET_OUT_EDGES)))
          .addMethod(
            METHOD_GET_IN_EDGES,
            asyncServerStreamingCall(
              new MethodHandlers<
                com.alibaba.maxgraph.proto.GetInEdgesRequest,
                com.alibaba.maxgraph.proto.EdgeProto>(
                  this, METHODID_GET_IN_EDGES)))
          .addMethod(
            METHOD_GET_SCHEMA,
            asyncUnaryCall(
              new MethodHandlers<
                com.alibaba.maxgraph.proto.GetSchemaRequest,
                com.alibaba.maxgraph.proto.SchemaProto>(
                  this, METHODID_GET_SCHEMA)))
          .build();
    }
  }

  /**
   */
  public static final class DebugServiceApiStub extends io.grpc.stub.AbstractStub<DebugServiceApiStub> {
    private DebugServiceApiStub(io.grpc.Channel channel) {
      super(channel);
    }

    private DebugServiceApiStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected DebugServiceApiStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new DebugServiceApiStub(channel, callOptions);
    }

    /**
     */
    public void getServerInfo(com.alibaba.maxgraph.proto.Empty request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.ServerInfo> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_GET_SERVER_INFO, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getGraphInfo(com.alibaba.maxgraph.proto.Empty request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.GraphInfo> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_GET_GRAPH_INFO, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getVertex(com.alibaba.maxgraph.proto.GetVertexRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.VertexProto> responseObserver) {
      asyncServerStreamingCall(
          getChannel().newCall(METHOD_GET_VERTEX, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void scanVertex(com.alibaba.maxgraph.proto.ScanVertexRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.VertexProto> responseObserver) {
      asyncServerStreamingCall(
          getChannel().newCall(METHOD_SCAN_VERTEX, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getOutEdges(com.alibaba.maxgraph.proto.GetOutEdgesRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.EdgeProto> responseObserver) {
      asyncServerStreamingCall(
          getChannel().newCall(METHOD_GET_OUT_EDGES, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getInEdges(com.alibaba.maxgraph.proto.GetInEdgesRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.EdgeProto> responseObserver) {
      asyncServerStreamingCall(
          getChannel().newCall(METHOD_GET_IN_EDGES, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getSchema(com.alibaba.maxgraph.proto.GetSchemaRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.SchemaProto> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_GET_SCHEMA, getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class DebugServiceApiBlockingStub extends io.grpc.stub.AbstractStub<DebugServiceApiBlockingStub> {
    private DebugServiceApiBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private DebugServiceApiBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected DebugServiceApiBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new DebugServiceApiBlockingStub(channel, callOptions);
    }

    /**
     */
    public com.alibaba.maxgraph.proto.ServerInfo getServerInfo(com.alibaba.maxgraph.proto.Empty request) {
      return blockingUnaryCall(
          getChannel(), METHOD_GET_SERVER_INFO, getCallOptions(), request);
    }

    /**
     */
    public com.alibaba.maxgraph.proto.GraphInfo getGraphInfo(com.alibaba.maxgraph.proto.Empty request) {
      return blockingUnaryCall(
          getChannel(), METHOD_GET_GRAPH_INFO, getCallOptions(), request);
    }

    /**
     */
    public java.util.Iterator<com.alibaba.maxgraph.proto.VertexProto> getVertex(
        com.alibaba.maxgraph.proto.GetVertexRequest request) {
      return blockingServerStreamingCall(
          getChannel(), METHOD_GET_VERTEX, getCallOptions(), request);
    }

    /**
     */
    public java.util.Iterator<com.alibaba.maxgraph.proto.VertexProto> scanVertex(
        com.alibaba.maxgraph.proto.ScanVertexRequest request) {
      return blockingServerStreamingCall(
          getChannel(), METHOD_SCAN_VERTEX, getCallOptions(), request);
    }

    /**
     */
    public java.util.Iterator<com.alibaba.maxgraph.proto.EdgeProto> getOutEdges(
        com.alibaba.maxgraph.proto.GetOutEdgesRequest request) {
      return blockingServerStreamingCall(
          getChannel(), METHOD_GET_OUT_EDGES, getCallOptions(), request);
    }

    /**
     */
    public java.util.Iterator<com.alibaba.maxgraph.proto.EdgeProto> getInEdges(
        com.alibaba.maxgraph.proto.GetInEdgesRequest request) {
      return blockingServerStreamingCall(
          getChannel(), METHOD_GET_IN_EDGES, getCallOptions(), request);
    }

    /**
     */
    public com.alibaba.maxgraph.proto.SchemaProto getSchema(com.alibaba.maxgraph.proto.GetSchemaRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_GET_SCHEMA, getCallOptions(), request);
    }
  }

  /**
   */
  public static final class DebugServiceApiFutureStub extends io.grpc.stub.AbstractStub<DebugServiceApiFutureStub> {
    private DebugServiceApiFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private DebugServiceApiFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected DebugServiceApiFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new DebugServiceApiFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.alibaba.maxgraph.proto.ServerInfo> getServerInfo(
        com.alibaba.maxgraph.proto.Empty request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_GET_SERVER_INFO, getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.alibaba.maxgraph.proto.GraphInfo> getGraphInfo(
        com.alibaba.maxgraph.proto.Empty request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_GET_GRAPH_INFO, getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.alibaba.maxgraph.proto.SchemaProto> getSchema(
        com.alibaba.maxgraph.proto.GetSchemaRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_GET_SCHEMA, getCallOptions()), request);
    }
  }

  private static final int METHODID_GET_SERVER_INFO = 0;
  private static final int METHODID_GET_GRAPH_INFO = 1;
  private static final int METHODID_GET_VERTEX = 2;
  private static final int METHODID_SCAN_VERTEX = 3;
  private static final int METHODID_GET_OUT_EDGES = 4;
  private static final int METHODID_GET_IN_EDGES = 5;
  private static final int METHODID_GET_SCHEMA = 6;

  private static class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final DebugServiceApiImplBase serviceImpl;
    private final int methodId;

    public MethodHandlers(DebugServiceApiImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_GET_SERVER_INFO:
          serviceImpl.getServerInfo((com.alibaba.maxgraph.proto.Empty) request,
              (io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.ServerInfo>) responseObserver);
          break;
        case METHODID_GET_GRAPH_INFO:
          serviceImpl.getGraphInfo((com.alibaba.maxgraph.proto.Empty) request,
              (io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.GraphInfo>) responseObserver);
          break;
        case METHODID_GET_VERTEX:
          serviceImpl.getVertex((com.alibaba.maxgraph.proto.GetVertexRequest) request,
              (io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.VertexProto>) responseObserver);
          break;
        case METHODID_SCAN_VERTEX:
          serviceImpl.scanVertex((com.alibaba.maxgraph.proto.ScanVertexRequest) request,
              (io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.VertexProto>) responseObserver);
          break;
        case METHODID_GET_OUT_EDGES:
          serviceImpl.getOutEdges((com.alibaba.maxgraph.proto.GetOutEdgesRequest) request,
              (io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.EdgeProto>) responseObserver);
          break;
        case METHODID_GET_IN_EDGES:
          serviceImpl.getInEdges((com.alibaba.maxgraph.proto.GetInEdgesRequest) request,
              (io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.EdgeProto>) responseObserver);
          break;
        case METHODID_GET_SCHEMA:
          serviceImpl.getSchema((com.alibaba.maxgraph.proto.GetSchemaRequest) request,
              (io.grpc.stub.StreamObserver<com.alibaba.maxgraph.proto.SchemaProto>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }
  }

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    return new io.grpc.ServiceDescriptor(SERVICE_NAME,
        METHOD_GET_SERVER_INFO,
        METHOD_GET_GRAPH_INFO,
        METHOD_GET_VERTEX,
        METHOD_SCAN_VERTEX,
        METHOD_GET_OUT_EDGES,
        METHOD_GET_IN_EDGES,
        METHOD_GET_SCHEMA);
  }

}
