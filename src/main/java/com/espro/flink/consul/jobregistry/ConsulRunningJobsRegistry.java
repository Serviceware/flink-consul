package com.espro.flink.consul.jobregistry;

import static java.text.MessageFormat.format;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.espro.flink.consul.metric.ConsulMetricService;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.highavailability.JobResultEntry;
import org.apache.flink.runtime.highavailability.JobResultStore;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.kv.model.GetValue;
import com.ecwid.consul.v1.kv.model.PutParams;
import com.espro.flink.consul.ConsulSessionHolder;

/**
 * Stores the status of a Flink Job in Consul.
 *
 * @see JobResultStore
 */
public final class ConsulRunningJobsRegistry implements JobResultStore {

	private static final String COMMA_SEPARATOR = ",";

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
	public void createDirtyResult(JobResultEntry jobResultEntry) throws IllegalStateException {
		storeJobStatus(jobResultEntry.getJobId(), JobStatus.DIRTY);
	}

	@Override
	public void markResultAsClean(JobID jobID) throws IOException, NoSuchElementException {
		storeJobStatus(jobID, JobStatus.CLEAN);
	}

	@Override
	public boolean hasDirtyJobResultEntry(JobID jobID) throws IOException {
		Set<String> jobResultEntries = getJobResultEntries(JobStatus.DIRTY);
		return checkJobsContainId(jobResultEntries, jobID);
	}

	@Override
	public boolean hasCleanJobResultEntry(JobID jobID) throws IOException {
		Set<String> jobResultEntries = getJobResultEntries(JobStatus.CLEAN);
		return checkJobsContainId(jobResultEntries, jobID);
	}

	@Override
	public Set<JobResult> getDirtyResults() throws IOException {
		Set<String> jobResultEntries = getJobResultEntries(JobStatus.DIRTY);
		return jobResultEntries.stream()
				.map(id -> new JobResult.Builder().jobId(new JobID(StringUtils.hexStringToByte(id))).netRuntime(1).build())
				.collect(Collectors.toSet());
	}

	private void storeJobStatus(JobID jobID, JobStatus  status) {
		PutParams params = new PutParams();
		params.setAcquireSession(sessionHolder.getSessionId());
		Set<String> jobList = getJobResultEntries(status);
		if (CollectionUtils.isEmpty(jobList)) {
			jobList = new HashSet<>();
		}
		jobList.add(jobID.toString());
		String jobIdsAsString = String.join(COMMA_SEPARATOR, jobList);
		LocalDateTime startTime = LocalDateTime.now();
		Boolean jobStatusStorageResult = client.get().setKVValue(path(status), jobIdsAsString, params).getValue();
		this.consulMetricService.updateWriteMetrics(startTime);
		if (jobStatusStorageResult == null || !jobStatusStorageResult) {
			throw new IllegalStateException(format("Failed to store JobStatus({0}) for JobID: {1}", status, jobID));
		}
	}

	private String path(JobStatus  status) {
		return jobRegistryPath + status.getValue();
	}

	private Set<String> getJobResultEntries(JobStatus jobStatus) {
		LocalDateTime startTime = LocalDateTime.now();
		GetValue value = client.get().getKVValue(path(jobStatus)).getValue();
		this.consulMetricService.updateReadMetrics(startTime);
		if (value == null) {
			return Collections.emptySet();
		}
		return convertToSet(value.getDecodedValue());
	}

	private boolean checkJobsContainId(Set<String> jobResultEntries, JobID jobID) {
		if (CollectionUtils.isEmpty(jobResultEntries)) {
			return false;
		}
		return jobResultEntries.contains(jobID.toString());
	}

	private static Set<String> convertToSet(String jobs) {
        if (jobs == null || jobs.isEmpty()) {
            return Collections.emptySet();
        }
		return Arrays.stream(jobs.split(COMMA_SEPARATOR)).collect(Collectors.toSet());
	}
}
