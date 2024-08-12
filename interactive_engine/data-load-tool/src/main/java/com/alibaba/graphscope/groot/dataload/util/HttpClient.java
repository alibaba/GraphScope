package com.alibaba.graphscope.groot.dataload.util;

import com.alibaba.graphscope.groot.common.exception.InvalidArgumentException;

import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.Map;

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

    public static String doGet(String url, Map<String, String> param) throws Exception {
        CloseableHttpClient httpClient = HttpClients.createDefault();
        String resultMsg = "";
        CloseableHttpResponse response = null;
        try {
            URIBuilder builder = new URIBuilder(url);
            if (param != null) {
                for (String key : param.keySet()) {
                    builder.addParameter(key, param.get(key));
                }
            }
            URI uri = builder.build();
            HttpGet httpGet = new HttpGet(uri);
            response = httpClient.execute(httpGet);
            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                resultMsg = EntityUtils.toString(response.getEntity(), "UTF-8");
            } else {
                throw new InvalidArgumentException(
                        "HTTP GET failed. code="
                                + response.getStatusLine().getStatusCode()
                                + ", msg="
                                + response.getStatusLine().getReasonPhrase());
            }
        } catch (Exception e) {
            throw new InvalidArgumentException("HTTP GET failed", e);
        } finally {
            try {
                if (response != null) {
                    response.close();
                }
                httpClient.close();
            } catch (IOException e) {
                throw new InvalidArgumentException("httpClient close failed", e);
            }
        }
        return resultMsg;
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
