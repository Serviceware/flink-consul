package com.espro.flink.consul.checkpoint;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import com.espro.flink.consul.metric.ConsulMetricService;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;
import org.apache.flink.shaded.guava18.com.google.common.base.Stopwatch;
import org.apache.flink.util.Preconditions;

import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.kv.model.GetValue;
import com.ecwid.consul.v1.kv.model.PutParams;

final class ConsulCheckpointIDCounter implements CheckpointIDCounter {

    private final Supplier<ConsulClient> clientProvider;
	private final String countersPath;
	private final JobID jobID;
	private long index;
	private final ConsulMetricService consulMetricService;

    public ConsulCheckpointIDCounter(Supplier<ConsulClient> clientProvider, String countersPath, JobID jobID, ConsulMetricService consulMetricService) {
        this.clientProvider = Preconditions.checkNotNull(clientProvider, "client");
		this.countersPath = Preconditions.checkNotNull(countersPath, "countersPath");
		this.jobID = Preconditions.checkNotNull(jobID, "jobID");
		Preconditions.checkArgument(countersPath.endsWith("/"), "countersPath must end with /");
		this.consulMetricService = consulMetricService;
	}

	@Override
	public void start() throws Exception {

	}

	@Override
	public void shutdown(JobStatus jobStatus) throws Exception {
		if (jobStatus.isGloballyTerminalState()) {
			removeCounter();
		}
	}

	@Override
	public long getAndIncrement() throws Exception {
		while (true) {
            long v = get();
			if (writeCounter(v + 1)) {
				return v;
			} else {
				Thread.sleep(100);
			}
		}
	}

    @Override
    public long get() {
		Stopwatch started = Stopwatch.createStarted();
        GetValue gv = clientProvider.get().getKVValue(counterKey()).getValue();
		this.consulMetricService.updateReadMetrics(started.elapsed(TimeUnit.MILLISECONDS));
        if (gv == null) {
            index = 0;
            return 0;
        } else {
            index = gv.getModifyIndex();
            return Long.valueOf(gv.getDecodedValue());
        }
    }

	@Override
	public void setCount(long newId) throws Exception {
		writeCounter(newId);
	}

	private boolean writeCounter(long value) {
		PutParams params = new PutParams();
		params.setCas(index);
		Stopwatch started = Stopwatch.createStarted();
		boolean result = clientProvider.get().setKVValue(counterKey(), String.valueOf(value), params).getValue();
		this.consulMetricService.updateReadMetrics(started.elapsed(TimeUnit.MILLISECONDS));
		return result;
	}

	private void removeCounter() {
        clientProvider.get().deleteKVValue(counterKey());
	}

	private String counterKey() {
		return countersPath + jobID.toString();
	}
}
