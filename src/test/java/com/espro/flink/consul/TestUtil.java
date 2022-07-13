package com.espro.flink.consul;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.plugin.PluginManager;
import org.apache.flink.core.plugin.PluginUtils;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.MetricRegistryConfiguration;
import org.apache.flink.runtime.metrics.MetricRegistryImpl;
import org.apache.flink.runtime.metrics.ReporterSetup;

public class TestUtil {

    /**
     * This method is an entry point to register a metric to the Flink metric by creating MetricRegistry.
     * */
    public static MetricRegistry createMetricRegistry(Configuration configuration) {
        PluginManager pluginManager =
                PluginUtils.createPluginManagerFromRootFolder(configuration);
        return new MetricRegistryImpl(
                MetricRegistryConfiguration.fromConfiguration(configuration),
                ReporterSetup.fromConfiguration(configuration, pluginManager));
    }
}
