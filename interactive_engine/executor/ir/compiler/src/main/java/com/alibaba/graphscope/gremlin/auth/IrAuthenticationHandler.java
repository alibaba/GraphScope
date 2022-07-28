/*
 * This file is referred and derived from project apache/tinkerpop
 *
 * https://github.com/apache/tinkerpop/blob/master/gremlin-server/src/main/java/org/apache/tinkerpop/gremlin/server/handler/SaslAuthenticationHandler.java
 *
 * which has the following license:
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.alibaba.graphscope.gremlin.auth;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.Attribute;
import io.netty.util.AttributeMap;

import org.apache.tinkerpop.gremlin.driver.Tokens;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.server.GremlinServer;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticatedUser;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticationException;
import org.apache.tinkerpop.gremlin.server.auth.Authenticator;
import org.apache.tinkerpop.gremlin.server.authz.Authorizer;
import org.apache.tinkerpop.gremlin.server.handler.AbstractAuthenticationHandler;
import org.apache.tinkerpop.gremlin.server.handler.SaslAuthenticationHandler;
import org.apache.tinkerpop.gremlin.server.handler.StateKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

// a channel handler for authentication, configurable in conf/gremlin-server.yaml
@ChannelHandler.Sharable
public class IrAuthenticationHandler extends AbstractAuthenticationHandler {
    private static final Logger logger = LoggerFactory.getLogger(SaslAuthenticationHandler.class);
    private static final Base64.Decoder BASE64_DECODER = Base64.getDecoder();
    private static final Base64.Encoder BASE64_ENCODER = Base64.getEncoder();
    private static final Logger auditLogger =
            LoggerFactory.getLogger(GremlinServer.AUDIT_LOGGER_NAME);

    protected final Settings settings;

    // Authenticator is configurable in conf/gremlin-server.yaml, currently {@link
    // DefaultAuthenticator}
    @Deprecated
    public IrAuthenticationHandler(final Authenticator authenticator, final Settings settings) {
        this(authenticator, null, settings);
    }

    public IrAuthenticationHandler(
            final Authenticator authenticator,
            final Authorizer authorizer,
            final Settings settings) {
        super(authenticator, authorizer);
        this.settings = settings;
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception {
        if (msg instanceof RequestMessage) {
            final RequestMessage requestMessage = (RequestMessage) msg;

            final Attribute<Authenticator.SaslNegotiator> negotiator =
                    ((AttributeMap) ctx).attr(StateKey.NEGOTIATOR);
            final Attribute<RequestMessage> request =
                    ((AttributeMap) ctx).attr(StateKey.REQUEST_MESSAGE);

            if (negotiator.get() == null) {
                try {
                    // First time through so save the request and send an AUTHENTICATE challenge
                    // with no data
                    negotiator.set(authenticator.newSaslNegotiator(getRemoteInetAddress(ctx)));
                    request.set(requestMessage);
                    // the authentication flag is off, just pass the original message down the
                    // pipeline for processing
                    if (!authenticator.requireAuthentication()) {
                        ctx.pipeline().remove(this);
                        final RequestMessage original = request.get();
                        ctx.fireChannelRead(original);
                    } else {
                        final ResponseMessage authenticate =
                                ResponseMessage.build(requestMessage)
                                        .code(ResponseStatusCode.AUTHENTICATE)
                                        .create();
                        ctx.writeAndFlush(authenticate);
                    }
                } catch (Exception ex) {
                    // newSaslNegotiator can cause troubles - if we don't catch and respond nicely
                    // the driver seems
                    // to hang until timeout which isn't so nice. treating this like a server error
                    // as it means that
                    // the Authenticator isn't really ready to deal with requests for some reason.
                    logger.error(
                            String.format(
                                    "%s is not ready to handle requests - check its configuration"
                                            + " or related services",
                                    authenticator.getClass().getSimpleName()),
                            ex);

                    final ResponseMessage error =
                            ResponseMessage.build(requestMessage)
                                    .statusMessage("Authenticator is not ready to handle requests")
                                    .code(ResponseStatusCode.SERVER_ERROR)
                                    .create();
                    ctx.writeAndFlush(error);
                }
            } else {
                if (requestMessage.getOp().equals(Tokens.OPS_AUTHENTICATION)
                        && requestMessage.getArgs().containsKey(Tokens.ARGS_SASL)) {

                    final Object saslObject = requestMessage.getArgs().get(Tokens.ARGS_SASL);
                    final byte[] saslResponse;

                    if (saslObject instanceof String) {
                        saslResponse = BASE64_DECODER.decode((String) saslObject);
                    } else {
                        final ResponseMessage error =
                                ResponseMessage.build(request.get())
                                        .statusMessage(
                                                "Incorrect type for : "
                                                        + Tokens.ARGS_SASL
                                                        + " - base64 encoded String is expected")
                                        .code(ResponseStatusCode.REQUEST_ERROR_MALFORMED_REQUEST)
                                        .create();
                        ctx.writeAndFlush(error);
                        return;
                    }

                    try {
                        final byte[] saslMessage = negotiator.get().evaluateResponse(saslResponse);
                        if (negotiator.get().isComplete()) {
                            final AuthenticatedUser user = negotiator.get().getAuthenticatedUser();
                            ctx.channel().attr(StateKey.AUTHENTICATED_USER).set(user);
                            // User name logged with the remote socket address and authenticator
                            // classname for audit logging
                            if (settings.enableAuditLog || settings.authentication.enableAuditLog) {
                                String address = ctx.channel().remoteAddress().toString();
                                if (address.startsWith("/") && address.length() > 1)
                                    address = address.substring(1);
                                final String[] authClassParts =
                                        authenticator.getClass().toString().split("[.]");
                                auditLogger.info(
                                        "User {} with address {} authenticated by {}",
                                        user.getName(),
                                        address,
                                        authClassParts[authClassParts.length - 1]);
                            }
                            // If we have got here we are authenticated so remove the handler and
                            // pass
                            // the original message down the pipeline for processing
                            ctx.pipeline().remove(this);
                            final RequestMessage original = request.get();
                            ctx.fireChannelRead(original);
                        } else {
                            // not done here - send back the sasl message for next challenge.
                            final Map<String, Object> metadata = new HashMap<>();
                            metadata.put(
                                    Tokens.ARGS_SASL, BASE64_ENCODER.encodeToString(saslMessage));
                            final ResponseMessage authenticate =
                                    ResponseMessage.build(requestMessage)
                                            .statusAttributes(metadata)
                                            .code(ResponseStatusCode.AUTHENTICATE)
                                            .create();
                            ctx.writeAndFlush(authenticate);
                        }
                    } catch (AuthenticationException ae) {
                        final ResponseMessage error =
                                ResponseMessage.build(request.get())
                                        .statusMessage(ae.getMessage())
                                        .code(ResponseStatusCode.UNAUTHORIZED)
                                        .create();
                        ctx.writeAndFlush(error);
                    }
                } else {
                    final ResponseMessage error =
                            ResponseMessage.build(requestMessage)
                                    .statusMessage("Failed to authenticate")
                                    .code(ResponseStatusCode.UNAUTHORIZED)
                                    .create();
                    ctx.writeAndFlush(error);
                }
            }
        } else {
            logger.warn(
                    "{} only processes RequestMessage instances - received {} - channel closing",
                    this.getClass().getSimpleName(),
                    msg.getClass());
            ctx.close();
        }
    }

    private InetAddress getRemoteInetAddress(final ChannelHandlerContext ctx) {
        final Channel channel = ctx.channel();

        if (null == channel) return null;

        final SocketAddress genericSocketAddr = channel.remoteAddress();

        if (null == genericSocketAddr || !(genericSocketAddr instanceof InetSocketAddress))
            return null;

        return ((InetSocketAddress) genericSocketAddr).getAddress();
    }
}
