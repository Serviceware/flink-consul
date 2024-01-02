/**
 * Copyright (c) SABIO GmbH, Hamburg 2023 - All rights reserved
 */
package com.espro.flink.consul;

import static com.espro.flink.consul.configuration.ConsulHighAvailabilityOptions.HA_CONSUL_HOST;
import static com.espro.flink.consul.configuration.ConsulHighAvailabilityOptions.HA_CONSUL_PORT;
import static com.espro.flink.consul.configuration.ConsulHighAvailabilityOptions.HA_CONSUL_TLS_ALGORITHM;
import static com.espro.flink.consul.configuration.ConsulHighAvailabilityOptions.HA_CONSUL_TLS_ENABLED;
import static com.espro.flink.consul.configuration.ConsulHighAvailabilityOptions.HA_CONSUL_TLS_KEYSTORE_PASSWORD;
import static com.espro.flink.consul.configuration.ConsulHighAvailabilityOptions.HA_CONSUL_TLS_KEYSTORE_PATH;
import static com.espro.flink.consul.configuration.ConsulHighAvailabilityOptions.HA_CONSUL_TLS_KEYSTORE_TYPE;
import static com.espro.flink.consul.configuration.ConsulHighAvailabilityOptions.HA_CONSUL_TLS_TRUSTSTORE_PASSWORD;
import static com.espro.flink.consul.configuration.ConsulHighAvailabilityOptions.HA_CONSUL_TLS_TRUSTSTORE_PATH;
import static com.espro.flink.consul.configuration.ConsulHighAvailabilityOptions.HA_CONSUL_TLS_TRUSTSTORE_TYPE;
import static org.apache.flink.util.Preconditions.checkNotNull;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.security.SecureRandom;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ecwid.consul.transport.TransportException;
import com.ecwid.consul.v1.ConsulClient;

import nl.altindag.ssl.SSLFactory;
import nl.altindag.ssl.util.SSLFactoryUtils;

/**
 * Implementation for providing a {@link ConsulClient} depending on the {@link Configuration}
 */
public class ConsulClientProviderImpl implements ConsulClientProvider, Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(ConsulClientProviderImpl.class);

    private final Configuration configuration;

    private final ScheduledExecutorService executorService;

    private volatile ConsulClient consulClient;

    private volatile Runnable sslUpdater;

    public ConsulClientProviderImpl(Configuration configuration) {
        this.configuration = configuration;
        this.executorService = Executors.newSingleThreadScheduledExecutor();
    }

    /**
     * Should be used just for testing!
     * @param consulClient
     */
    public ConsulClientProviderImpl(ConsulClient consulClient) {
        this((Configuration) null);
        this.consulClient = consulClient;
        this.sslUpdater = () -> LOG.info("Did nothing");
    }

    /**
     * Initializes the {@link ConsulClientProviderImpl} by creating internally the {@link ConsulClient}
     */
    @Override
    public void init() {
        String consulHost = configuration.getString(HA_CONSUL_HOST);
        int consulPort = configuration.getInteger(HA_CONSUL_PORT);

        if (configuration.getBoolean(HA_CONSUL_TLS_ENABLED)) {
            SSLFactory sslContextFactory = createSSlFactory(configuration);

            sslUpdater = () -> {
                try {
                    SSLFactory updatedSslFactory = createSSlFactory(configuration);
                    SSLFactoryUtils.reload(sslContextFactory, updatedSslFactory);
                } catch (Exception e) {
                    LOG.error("Error while updating ssl context for communication with Consul.");
                }
            };
            // initial update
            sslUpdater.run();

            // SSL context is updated every 5 minutes
            executorService.scheduleAtFixedRate(sslUpdater, 5, 5, TimeUnit.MINUTES);

            consulClient = ConsulClientFactory.createSecuredHttpClient(consulHost, consulPort, sslContextFactory, configuration);
        } else {
            consulClient = ConsulClientFactory.createConsulClient(consulHost, consulPort);
        }
    }

    private static SSLFactory createSSlFactory(Configuration configuration) {
        String keyStorePath = checkNotNull(configuration.getString(HA_CONSUL_TLS_KEYSTORE_PATH), "No keystore path given!");
        String keyStorePassword = configuration.getString(HA_CONSUL_TLS_KEYSTORE_PASSWORD);
        String keyStoreType = checkNotNull(configuration.getString(HA_CONSUL_TLS_KEYSTORE_TYPE), "No keystore type given!");

        String trustStorePath = checkNotNull(configuration.getString(HA_CONSUL_TLS_TRUSTSTORE_PATH), "No truststore path given!");
        String trustStorePassword = configuration.getString(HA_CONSUL_TLS_TRUSTSTORE_PASSWORD);
        String trustStoreType = checkNotNull(configuration.getString(HA_CONSUL_TLS_TRUSTSTORE_TYPE), "No truststore type given!");

        String protocol = configuration.getString(HA_CONSUL_TLS_ALGORITHM);

        char[] keyStorePasswordCharArray = keyStorePassword != null ? keyStorePassword.toCharArray() : null;
        char[] trustStorePasswordCharArray = trustStorePassword != null ? trustStorePassword.toCharArray() : null;

        return SSLFactory.builder()
                .withIdentityMaterial(Paths.get(URI.create(keyStorePath)), keyStorePasswordCharArray, trustStorePasswordCharArray, keyStoreType)
                .withTrustMaterial(Paths.get(URI.create(trustStorePath)), trustStorePasswordCharArray, trustStoreType)
                .withSslContextAlgorithm(protocol)
                .withSecureRandom(new SecureRandom())
                .withSwappableIdentityMaterial()
                .withSwappableTrustMaterial()
                .build();
    }

    @Override
    public ConsulClient get() {
        return consulClient;
    }

    @Override
    public <T> T executeWithSslRecovery(Function<ConsulClient, T> runner) {
        try {
            return runner.apply(consulClient);
        } catch (TransportException e) {
            LOG.info("Try to update certificates due to a TransportException occured while calling Consul.");
            sslUpdater.run();
            return runner.apply(consulClient);
        }
    }

    @Override
    public void close() throws IOException {
        // There is no need to wait for execution of scheduled tasks
        LOG.info("Shuting down executorService now for renewing the ssl context...");
        executorService.shutdownNow();
        try {
            executorService.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
