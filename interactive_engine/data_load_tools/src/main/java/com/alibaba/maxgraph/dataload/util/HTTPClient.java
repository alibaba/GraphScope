package com.alibaba.maxgraph.dataload.util;

import java.io.IOException;
import java.net.Authenticator;
import java.net.HttpURLConnection;
import java.net.PasswordAuthentication;
import java.net.URL;
import java.nio.charset.StandardCharsets;

import org.apache.commons.codec.binary.Base64;

/**
 * Simple HTTPClient which refer from <a href="https://github.com/eugenp/tutorials/blob/master/core-java-modules/core-java-networking-2/src/main/java/com/baeldung/url/auth/HttpClient.java">...</a>
 */
public class HttpClient {

    private final String user;
    private final String password;

    public HttpClient() {
        this("", "");
    }
    public HttpClient(String user, String password) {
        this.user = user;
        this.password = password;
    }

    public int sendRequestWithAuthHeader(String url) throws IOException {
        HttpURLConnection connection = null;
        try {
            connection = createConnection(url);
            connection.setRequestProperty("Authorization", createBasicAuthHeaderValue());
            return connection.getResponseCode();
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }

    public int sendRequestWithAuthenticator(String url) throws IOException {
        setAuthenticator();
        HttpURLConnection connection = null;
        try {
            connection = createConnection(url);
            return connection.getResponseCode();
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }

    public HttpURLConnection createConnection(String urlString) throws IOException {
        URL url = new URL(urlString);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("GET");
        return connection;
    }

    private String createBasicAuthHeaderValue() {
        String auth = user + ":" + password;
        byte[] encodedAuth = Base64.encodeBase64(auth.getBytes(StandardCharsets.UTF_8));
        return "Basic " + new String(encodedAuth);
    }

    private void setAuthenticator() {
        Authenticator.setDefault(new BasicAuthenticator());
    }

    private final class BasicAuthenticator extends Authenticator {
        protected PasswordAuthentication getPasswordAuthentication() {
            return new PasswordAuthentication(user, password.toCharArray());
        }
    }
}
