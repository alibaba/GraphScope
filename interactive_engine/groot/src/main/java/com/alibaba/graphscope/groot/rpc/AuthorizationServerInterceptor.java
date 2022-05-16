package com.alibaba.graphscope.groot.rpc;

import com.alibaba.maxgraph.sdkcommon.BasicAuth;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCall.Listener;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import java.util.Base64;
import java.util.Map;

public class AuthorizationServerInterceptor implements ServerInterceptor {

    private Map<String, String> validUsers;

    public AuthorizationServerInterceptor(Map<String, String> validUsers) {
        this.validUsers = validUsers;
    }

    @Override
    public <ReqT, RespT> Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> serverCall,
            Metadata metadata,
            ServerCallHandler<ReqT, RespT> serverCallHandler) {
        String authToken = metadata.get(BasicAuth.AUTH_KEY);
        if (authToken == null) {
            serverCall.close(
                    Status.UNAUTHENTICATED.withDescription("No authorization token"), metadata);
        } else if (!authToken.startsWith(BasicAuth.TOKEN_HEAD)) {
            serverCall.close(
                    Status.UNAUTHENTICATED.withDescription("invalid auth token"), metadata);
        } else {
            String token = authToken.substring(BasicAuth.TOKEN_HEAD.length());
            byte[] decodedBytes = Base64.getDecoder().decode(token);
            String decodedString = new String(decodedBytes);
            String[] authInfo = decodedString.split(":");
            String password = validUsers.get(authInfo[0]);
            if (!authInfo[1].equals(password)) {
                serverCall.close(
                        Status.UNAUTHENTICATED.withDescription("invalid username or password"),
                        metadata);
            }
        }
        return serverCallHandler.startCall(serverCall, metadata);
    }
}
