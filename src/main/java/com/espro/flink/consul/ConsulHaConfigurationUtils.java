/*
 * Copyright (c) SABIO GmbH, Hamburg 2022 - All rights reserved
 */
package com.espro.flink.consul;

import org.apache.flink.configuration.Configuration;

import com.espro.flink.consul.configuration.ConsulHighAvailabilityOptions;

/**
 * Provides utility methods to collect all configuration related functions.
 */
public final class ConsulHaConfigurationUtils {

    private ConsulHaConfigurationUtils() {
        // utility class
    }

    public static String jobStatusPathFromConfiguration(Configuration configuration) {
        return addPathSeparatorIfNecessary(configuration.getString(ConsulHighAvailabilityOptions.HA_CONSUL_ROOT))
                + addPathSeparatorIfNecessary(configuration.getString(ConsulHighAvailabilityOptions.HA_CONSUL_JOBSTATUS_PATH));
    }

    public static String leaderPathFromConfiguration(Configuration configuration) {
        return addPathSeparatorIfNecessary(configuration.getString(ConsulHighAvailabilityOptions.HA_CONSUL_ROOT))
                + addPathSeparatorIfNecessary(configuration.getString(ConsulHighAvailabilityOptions.HA_CONSUL_LEADER_PATH));
    }

    public static String jobGraphsPathFromConfiguration(Configuration configuration) {
        return addPathSeparatorIfNecessary(configuration.getString(ConsulHighAvailabilityOptions.HA_CONSUL_ROOT))
                + addPathSeparatorIfNecessary(configuration.getString(ConsulHighAvailabilityOptions.HA_CONSUL_JOBGRAPHS_PATH));
    }

    private static String addPathSeparatorIfNecessary(String path) {
        return path.endsWith("/") ? path : path + "/";
    }
}
