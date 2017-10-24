/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.pentaho.di.cluster;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.config.SocketConfig;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.X509TrustManager;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

/**
 * Encapsulates the Apache commons HTTP connection manager with a singleton. We can use this to limit the number of open
 * connections to slave servers.
 *
 * @author matt
 */
public class SlaveConnectionManager {
    static final int KETTLE_HTTPCLIENT_MAX_CONNECTIONS_PER_HOST
            = Integer.parseInt(System.getProperty("KETTLE_HTTPCLIENT_MAX_CONNECTIONS_PER_HOST", "100"));
    static final int KETTLE_HTTPCLIENT_MAX_CONNECTIONS
            = Integer.parseInt(System.getProperty("KETTLE_HTTPCLIENT_MAX_CONNECTIONS", "200"));
    static final boolean KETTLE_HTTPCLIENT_STALE_CHECKING
            = "Y".equalsIgnoreCase(System.getProperty("KETTLE_HTTPCLIENT_STALE_CHECKING", "Y"));

    static final int KETTLE_HTTPCLIENT_CONNECTION_TIMEOUT
            = Integer.parseInt(System.getProperty("KETTLE_HTTPCLIENT_CONNECTION_TIMEOUT", "5"));

    static final int KETTLE_HTTPCLIENT_CONNECTION_MANAGER_TIMEOUT = KETTLE_HTTPCLIENT_CONNECTION_TIMEOUT;
    static final int KETTLE_HTTPCLIENT_SOCKET_TIMEOUT
            = Integer.parseInt(System.getProperty("KETTLE_HTTPCLIENT_SOCKET_TIMEOUT", "30"));
    static final int KETTLE_HTTPCLIENT_SOCKET_LINGER
            = Integer.parseInt(System.getProperty("KETTLE_HTTPCLIENT_SOCKET_LINGER", "15"));

    private static final String SSL = "SSL";
    private static final String KEYSTORE_SYSTEM_PROPERTY = "javax.net.ssl.keyStore";

    private static SlaveConnectionManager slaveConnectionManager;

    private final PoolingHttpClientConnectionManager manager;
    private final RequestConfig defaultRequestConfig;

    private SlaveConnectionManager() {
        if (needToInitializeSSLContext()) {
            try {
                SSLContext context = SSLContext.getInstance(SSL);
                context.init(new KeyManager[0], new X509TrustManager[]{getDefaultTrustManager()}, new SecureRandom());
                SSLContext.setDefault(context);
            } catch (Exception e) {
                //log.logError( "Default SSL context hasn't been initialized", e );
            }
        }

        manager = new PoolingHttpClientConnectionManager();
        manager.setDefaultMaxPerRoute(KETTLE_HTTPCLIENT_MAX_CONNECTIONS_PER_HOST);
        manager.setMaxTotal(KETTLE_HTTPCLIENT_MAX_CONNECTIONS);

        manager.setDefaultSocketConfig(
                SocketConfig.custom()
                        .setSoTimeout(KETTLE_HTTPCLIENT_SOCKET_TIMEOUT)
                        .setSoLinger(KETTLE_HTTPCLIENT_SOCKET_LINGER).build());

        defaultRequestConfig = RequestConfig.custom()
                .setConnectTimeout(KETTLE_HTTPCLIENT_CONNECTION_MANAGER_TIMEOUT * 1000)
                .build();
    }

    private static boolean needToInitializeSSLContext() {
        return System.getProperty(KEYSTORE_SYSTEM_PROPERTY) == null;
    }

    public static SlaveConnectionManager getInstance() {
        if (slaveConnectionManager == null) {
            slaveConnectionManager = new SlaveConnectionManager();
        }
        return slaveConnectionManager;
    }

    public HttpClient createHttpClient() {
        return HttpClients.custom().setConnectionManager(manager)
                .setDefaultRequestConfig(defaultRequestConfig)
                .build();
    }

    public HttpClient createHttpClient(String user, String password) {
        CredentialsProvider provider = new BasicCredentialsProvider();
        UsernamePasswordCredentials credentials = new UsernamePasswordCredentials(user, password);
        provider.setCredentials(AuthScope.ANY, credentials);

        return
                HttpClientBuilder
                        .create()
                        .setDefaultCredentialsProvider(provider)
                        .setConnectionManager(manager)
                        .setDefaultRequestConfig(defaultRequestConfig)
                        .build();
    }

    public HttpClient createHttpClient(String user, String password,
                                       String proxyHost, int proxyPort, AuthScope authScope) {
        HttpHost httpHost = new HttpHost(proxyHost, proxyPort);

        RequestConfig requestConfig = RequestConfig.custom()
                .setProxy(httpHost)
                .setConnectTimeout(defaultRequestConfig.getConnectTimeout())
                .build();

        CredentialsProvider provider = new BasicCredentialsProvider();
        UsernamePasswordCredentials credentials = new UsernamePasswordCredentials(user, password);
        provider.setCredentials(authScope, credentials);

        return
                HttpClientBuilder
                        .create()
                        .setDefaultCredentialsProvider(provider)
                        .setDefaultRequestConfig(requestConfig)
                        .setConnectionManager(manager)
                        .build();
    }

    public void shutdown() {
        manager.shutdown();
    }

    private static X509TrustManager getDefaultTrustManager() {
        return new X509TrustManager() {
            @Override
            public void checkClientTrusted(X509Certificate[] certs, String param) throws CertificateException {
            }

            @Override
            public void checkServerTrusted(X509Certificate[] certs, String param) throws CertificateException {
                for (X509Certificate cert : certs) {
                    cert.checkValidity(); // validate date
                    // cert.verify( key ); // check by Public key
                    // cert.getBasicConstraints()!=-1 // check by CA
                }
            }

            @Override
            public X509Certificate[] getAcceptedIssuers() {
                return null;
            }
        };
    }

    static void reset() {
        slaveConnectionManager = null;
    }
}
