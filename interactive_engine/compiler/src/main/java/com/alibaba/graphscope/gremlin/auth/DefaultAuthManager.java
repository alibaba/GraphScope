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

package com.alibaba.graphscope.gremlin.auth;

import com.alibaba.graphscope.common.config.AuthConfig;
import com.alibaba.graphscope.common.config.Configs;

import org.apache.tinkerpop.gremlin.server.auth.AuthenticatedUser;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticationException;

public class DefaultAuthManager implements AuthManager<AuthenticatedUser> {
    private Configs configs;

    public DefaultAuthManager(Configs configs) {
        this.configs = configs;
    }

    // authenticate the username and the password by comparing with the expected from configs
    @Override
    public AuthenticatedUser authenticate(String userName, String password)
            throws AuthenticationException {
        String authUserName = AuthConfig.AUTH_USERNAME.get(configs);
        String authPassword = AuthConfig.AUTH_PASSWORD.get(configs);
        // no authentication
        if (!requireAuthentication()) {
            return AuthenticatedUser.ANONYMOUS_USER;
        }
        if (!authUserName.equals(userName) || !authPassword.equals(password)) {
            throw new AuthenticationException("user " + userName + " is invalid");
        }
        return new AuthenticatedUser(userName);
    }

    // if either of auth.username or auth.password is not set, disable the authentication
    @Override
    public boolean requireAuthentication() {
        String authUserName = AuthConfig.AUTH_USERNAME.get(configs);
        String authPassword = AuthConfig.AUTH_PASSWORD.get(configs);
        return !(authUserName == null || authUserName.isEmpty()) && !(authPassword == null || authPassword.isEmpty());
    }
}
