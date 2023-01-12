package com.espro.flink.consul;

import java.util.concurrent.TimeUnit;

import org.apache.flink.configuration.Configuration;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.LayeredConnectionSocketFactory;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

import com.ecwid.consul.transport.TransportException;
import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.ConsulRawClient;
import com.espro.flink.consul.configuration.ConsulHighAvailabilityOptions;

import nl.altindag.ssl.SSLFactory;
import nl.altindag.ssl.util.Apache4SslUtils;

/**
 * Factory class for creating a {@link ConsulClient} using the Flink {@link Configuration}.
 *
 * @see ConsulHighAvailabilityOptions
 */
final class ConsulClientFactory {

    private ConsulClientFactory() {
        // utility class for creating consul client
    }

    /**
     * Creates a {@link ConsulClient} using the configured consul properties out of the Flink {@link Configuration}.
     * 
     * @param consulHost host for connecting to Consul
     * @param consulPort port for connecting to Consul
     * @param configuration flink's configuration
     * @return {@link ConsulClient} using http
     */
    public static ConsulClient createConsulClient(String consulHost, int consulPort) {
        return new ConsulClient(consulHost, consulPort);
    }

    /**
     * 
     * @param consulHost host for connecting to Consul
     * @param consulPort port for connecting to Consul
     * @param sslFactory {@link SSLFactory} that is used for providing the ssl context
     * @param configuration configuration of the Flink environment
     * @return {@link ConsulClient} using https
     */
    public static ConsulClient createSecuredHttpClient(String consulHost, int consulPort, SSLFactory sslFactory, Configuration configuration) {
        Integer connectTimeout = configuration.getInteger(ConsulHighAvailabilityOptions.HA_CONSUL_CLIENT_CONNECT_TIMEOUT);
        Integer connectionRequestTimeout = configuration.getInteger(ConsulHighAvailabilityOptions.HA_CONSUL_CLIENT_CONNECTION_REQUEST_TIMEOUT);
        Integer socketTimeout = configuration.getInteger(ConsulHighAvailabilityOptions.HA_CONSUL_CLIENT_SOCKET_TIMEOUT);

        try {
            LayeredConnectionSocketFactory socketFactory = Apache4SslUtils.toSocketFactory(sslFactory);
            Registry<ConnectionSocketFactory> registry = RegistryBuilder.<ConnectionSocketFactory>create()
                    .register("https", socketFactory)
                    .build();

            PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager(registry);
            connectionManager.setMaxTotal(20);
            connectionManager.setDefaultMaxPerRoute(10);

            // Took timeout settings from AbstractHttpTransport
            RequestConfig requestConfig = RequestConfig.custom()
                    .setConnectTimeout((int) TimeUnit.SECONDS.toMillis(connectTimeout))
                    .setConnectionRequestTimeout((int) TimeUnit.SECONDS.toMillis(connectionRequestTimeout))
                    .setSocketTimeout((int) TimeUnit.SECONDS.toMillis(socketTimeout))
                    .build();

            HttpClientBuilder httpClientBuilder = HttpClientBuilder.create()
                    .disableAutomaticRetries()
                    .setConnectionManager(connectionManager)
                    .setDefaultRequestConfig(requestConfig);

            return new ConsulClient(new ConsulRawClient(consulHost, consulPort, httpClientBuilder.build()));
        } catch (Exception e) {
            throw new TransportException(e);
        }
    }
}
