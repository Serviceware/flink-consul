package com.espro.flink.consul.metric;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class registers metrics to Flink metrics and then calculates and publishes them.
 */
public class ConsulMetricService{
    private static final Logger LOG = LoggerFactory.getLogger(ConsulMetricService.class);

    private static final int HISTORY_SIZE = 100;

    private final ConsulMetricGroup consulMetricGroup;
    private final Counter readRequest;
    private final Counter writeRequest;
    private final Histogram readDuration;
    private final Histogram writeDuration;
    private final Counter session;
    private final Counter sessionError;
    private final Histogram sessionDuration;

    public ConsulMetricService(ConsulMetricGroup consulMetricGroup) {
        this.consulMetricGroup = consulMetricGroup;
        this.readRequest = new SimpleCounter();
        this.writeRequest = new SimpleCounter();
        this.session = new SimpleCounter();
        this.sessionError = new SimpleCounter();
        this.readDuration = new DescriptiveStatisticsHistogram(HISTORY_SIZE);
        this.writeDuration = new DescriptiveStatisticsHistogram(HISTORY_SIZE);
        this.sessionDuration = new DescriptiveStatisticsHistogram(HISTORY_SIZE);
        registerDefaultMetrics();
    }

    /**
     * Notifies about read operation on Consul's key/value store and how long it has taken
     * @param duration duration of read on Consul's key/value store
     */
    public void updateReadMetrics(long duration) {
        LOG.debug("Update read metrics.");
        synchronized (readRequest) {
            this.readRequest.inc();
            this.readDuration.update(duration);
        }
    }

    /**
     * Notifies about read operation on Consul's key/value store and how long it has taken
     * @param duration duration of write on Consul's key/value store
     */
    public void updateWriteMetrics(long duration) {
        LOG.debug("Update write metrics.");
        synchronized (writeRequest) {
            this.writeRequest.inc();
            this.writeDuration.update(duration);
        }
    }

    /**
     * Update the consul session metrics
     * @param duration duration of session on Consul's key/value store
     */
    public void updateSessionMetrics(long duration, boolean isError) {
        LOG.debug("Update session metrics.");
        synchronized (sessionDuration) {
            if (isError) {
                this.sessionError.inc();
            } else {
                this.session.inc();
            }
            this.sessionDuration.update(duration);
        }
    }

    /**
     * Register the default Consul metrics to the Flink metric entry point
     */
    private void registerDefaultMetrics() {
        LOG.info("Start registering default consul metrics.");
        consulMetricGroup.counter("consul.read", readRequest);
        consulMetricGroup.counter("consul.write", writeRequest);
        consulMetricGroup.histogram("consul.read.duration", readDuration);
        consulMetricGroup.histogram("consul.write.duration", writeDuration);
        consulMetricGroup.counter("consul.session", session);
        consulMetricGroup.counter("consul.session.error", sessionError);
        consulMetricGroup.histogram("consul.session.duration", sessionDuration);
    }
}
