package com.espro.flink.consul;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.plugin.PluginManager;
import org.apache.flink.core.plugin.PluginUtils;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.MetricRegistryConfiguration;
import org.apache.flink.runtime.metrics.MetricRegistryImpl;
import org.apache.flink.runtime.metrics.ReporterSetup;
import org.apache.flink.runtime.rpc.RpcSystem;

public class TestUtil {

    /**
     * This method is an entry point to register a metric to the Flink metric by creating MetricRegistry.
     * */
    public static MetricRegistry createMetricRegistry(Configuration configuration) {
        RpcSystem rpcSystem = RpcSystem.load(configuration);
        PluginManager pluginManager =
                PluginUtils.createPluginManagerFromRootFolder(configuration);
        return new MetricRegistryImpl(
                MetricRegistryConfiguration.fromConfiguration(
                        configuration, rpcSystem.getMaximumMessageSizeInBytes(configuration)),
                ReporterSetup.fromConfiguration(configuration, pluginManager));
    }
}
