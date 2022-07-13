package com.espro.flink.consul.jobregistry;

import static java.text.MessageFormat.format;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import com.espro.flink.consul.metric.ConsulMetricService;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.highavailability.RunningJobsRegistry;
import org.apache.flink.shaded.guava18.com.google.common.base.Stopwatch;
import org.apache.flink.util.Preconditions;

import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.kv.model.GetValue;
import com.ecwid.consul.v1.kv.model.PutParams;
import com.espro.flink.consul.ConsulSessionHolder;

/**
 * Stores the status of a Flink Job in Consul.
 *
 * @see JobSchedulingStatus
 */
public final class ConsulRunningJobsRegistry implements RunningJobsRegistry {

    private final Supplier<ConsulClient> client;
	private final ConsulSessionHolder sessionHolder;
	private final String jobRegistryPath;
	private final ConsulMetricService consulMetricService;

    public ConsulRunningJobsRegistry(Supplier<ConsulClient> client, ConsulSessionHolder sessionHolder, String jobRegistryPath, ConsulMetricService consulMetricService) {
		this.client = Preconditions.checkNotNull(client, "client");
		this.sessionHolder = Preconditions.checkNotNull(sessionHolder, "sessionHolder");
		this.jobRegistryPath = Preconditions.checkNotNull(jobRegistryPath, "jobRegistryPath");
		Preconditions.checkArgument(jobRegistryPath.endsWith("/"), "jobRegistryPath must end with /");
		this.consulMetricService = consulMetricService;
	}

	@Override
	public void setJobRunning(JobID jobID) throws IOException {
		storeJobStatus(jobID, JobSchedulingStatus.RUNNING);
	}

	@Override
	public void setJobFinished(JobID jobID) throws IOException {
		storeJobStatus(jobID, JobSchedulingStatus.DONE);
	}

	@Override
	public JobSchedulingStatus getJobSchedulingStatus(JobID jobID) throws IOException {
		Stopwatch started = Stopwatch.createStarted();
        GetValue value = client.get().getKVValue(path(jobID)).getValue();
		this.consulMetricService.updateReadMetrics(started.elapsed(TimeUnit.MILLISECONDS));
		return value == null ? JobSchedulingStatus.PENDING : JobSchedulingStatus.valueOf(value.getDecodedValue());
	}

	@Override
	public void clearJob(JobID jobID) throws IOException {
        client.get().deleteKVValue(path(jobID));
	}

	private void storeJobStatus(JobID jobID, JobSchedulingStatus status) {
		PutParams params = new PutParams();
		params.setAcquireSession(sessionHolder.getSessionId());
		Stopwatch started = Stopwatch.createStarted();
		Boolean jobStatusStorageResult = client.get().setKVValue(path(jobID), status.name(), params).getValue();
		this.consulMetricService.updateWriteMetrics(started.elapsed(TimeUnit.MILLISECONDS));
        if (jobStatusStorageResult == null || !jobStatusStorageResult) {
            throw new IllegalStateException(format("Failed to store JobStatus({0}) for JobID: {1}", status, jobID));
		}
	}

	private String path(JobID jobID) {
		return jobRegistryPath + jobID.toString();
	}
}
