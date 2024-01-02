package com.espro.flink.consul.checkpoint;

import java.util.concurrent.CompletableFuture;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.FutureUtils;

import com.ecwid.consul.v1.kv.model.GetValue;
import com.ecwid.consul.v1.kv.model.PutParams;
import com.espro.flink.consul.ConsulClientProvider;

final class ConsulCheckpointIDCounter implements CheckpointIDCounter {

    private final ConsulClientProvider clientProvider;
	private final String countersPath;
	private final JobID jobID;
	private long index;

    public ConsulCheckpointIDCounter(ConsulClientProvider clientProvider, String countersPath, JobID jobID) {
        this.clientProvider = Preconditions.checkNotNull(clientProvider, "client");
		this.countersPath = Preconditions.checkNotNull(countersPath, "countersPath");
		this.jobID = Preconditions.checkNotNull(jobID, "jobID");
		Preconditions.checkArgument(countersPath.endsWith("/"), "countersPath must end with /");
	}

	@Override
	public void start() throws Exception {

	}

	@Override
	public CompletableFuture<Void> shutdown(JobStatus jobStatus) {
		if (jobStatus.isGloballyTerminalState()) {
			removeCounter();
		}
		return FutureUtils.completedVoidFuture();
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
        GetValue gv = clientProvider.executeWithSslRecovery(consulClient -> consulClient.getKVValue(counterKey()).getValue());
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
        return clientProvider.executeWithSslRecovery(consulClient -> consulClient.setKVValue(counterKey(), String.valueOf(value), params).getValue());
	}

	private void removeCounter() {
        clientProvider.executeWithSslRecovery(consulClient -> consulClient.deleteKVValue(counterKey()));
	}

	private String counterKey() {
		return countersPath + jobID.toString();
	}
}
