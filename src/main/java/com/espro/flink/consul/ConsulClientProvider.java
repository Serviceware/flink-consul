/**
 * Copyright (c) SABIO GmbH, Hamburg 2024 - All rights reserved
 */
package com.espro.flink.consul;

import java.util.function.Function;
import java.util.function.Supplier;

import com.ecwid.consul.v1.ConsulClient;

/**
 *
 */
public interface ConsulClientProvider extends Supplier<ConsulClient> {

    /**
     * Initializes the internal consul client.
     */
    void init();

    /**
     * Could be used for handling transport exception when calling Consul.
     * @param <T> response of the executed Consul action
     * @param runner calls a method on the internally provided {@link ConsulClient}
     * @return response of the runner function
     */
    <T> T executeWithSslRecovery(Function<ConsulClient, T> runner);
}
