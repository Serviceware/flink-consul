package com.espro.flink.consul.metric;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.LocalDateTime;

/**
 * This class registers metrics to Flink metrics and then calculates and publishes them.
 */
public class ConsulMetricService{
    private static final Logger LOG = LoggerFactory.getLogger(ConsulMetricService.class);

    private static final int HISTORY_SIZE = 100;

    private final MetricRegistry metricRegistry;
    private final ConsulMetricGroup consulMetricGroup;
    private final Counter readRequest;
    private final Counter writeRequest;
    private final Histogram readDuration;
    private final Histogram writeDuration;
    private final Counter session;
    private final Counter sessionError;
    private final Histogram sessionDuration;

    public ConsulMetricService(MetricRegistry metricRegistry, ConsulMetricGroup consulMetricGroup) {
        this.metricRegistry = metricRegistry;
        this.consulMetricGroup = consulMetricGroup;
        this.readRequest = new SimpleCounter();
        this.writeRequest = new SimpleCounter();
        this.session = new SimpleCounter();
        this.sessionError = new SimpleCounter();
        this.readDuration = new DescriptiveStatisticsHistogram(HISTORY_SIZE);
        this.writeDuration = new DescriptiveStatisticsHistogram(HISTORY_SIZE);
        this.sessionDuration = new DescriptiveStatisticsHistogram(HISTORY_SIZE);
    }

    /**
     * Register the default Consul metrics to the Flink metric entry point
     */
    public void registerDefaultMetrics() {
        LOG.info("Start registering default consul metrics.");
        registerMetrics(readRequest, "consul.read");
        registerMetrics(writeRequest, "consul.write");
        registerMetrics(readDuration, "consul.read.duration");
        registerMetrics(writeDuration, "consul.write.duration");
        registerMetrics(session, "consul.session");
        registerMetrics(sessionError, "consul.session.error");
        registerMetrics(sessionDuration, "consul.session.duration");
    }

    /**
     * Register the Consul metrics to the Flink metric entry point
     * @param metric metric(e.g Counter, Histogram, etc)
     * @param metricName metric name
     */
    public void registerMetrics(Metric metric, String metricName) {
        this.metricRegistry.register(metric, metricName, consulMetricGroup);
    }

    /**
     * Update the consul read metrics
     * @param startTime start time of request
     */
    public void updateReadMetrics(LocalDateTime startTime) {
        LOG.debug("Update read metrics.");
        synchronized (readRequest) {
            this.readRequest.inc();
            this.readDuration.update(calculateDurationTime(startTime));
        }
    }

    /**
     * Update the consul write metrics
     * @param startTime start time of request
     */
    public void updateWriteMetrics(LocalDateTime startTime) {
        LOG.debug("Update write metrics.");
        synchronized (writeRequest) {
            this.writeRequest.inc();
            this.writeDuration.update(calculateDurationTime(startTime));
        }
    }

    /**
     * Update the consul session metrics
     * @param startTime start time of request
     */
    public void updateSessionMetrics(LocalDateTime startTime, boolean isError) {
        LOG.debug("Update session metrics.");
        synchronized (sessionDuration) {
            if (isError) {
                this.sessionError.inc();
            } else {
                this.session.inc();
            }
            this.sessionDuration.update(calculateDurationTime(startTime));
        }
    }

    private long calculateDurationTime(LocalDateTime startTime) {
        LocalDateTime currentTime = LocalDateTime.now();
        Duration duration = Duration.between(startTime, currentTime);
        return duration.toMillis() / 1000;
    }
}
