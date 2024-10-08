/*
 * Copyright (c) SABIO GmbH, Hamburg 2020 - All rights reserved
 */
package com.espro.flink.consul.checkpoint;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.checkpoint.CheckpointProperties;
import org.apache.flink.runtime.checkpoint.CheckpointRetentionPolicy;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.runtime.state.testutils.TestCompletedCheckpointStorageLocation;

/**
 * Some of these methods and class are taken from flink test module. Providing simple use of the checkpointing.
 */
public class CheckpointTestHelper {

    /**
     * Taken from {@link CompletedCheckpointStoreTest}.
     *
     * @param id - checkpijt id
     * @param sharedStateRegistry - shared state registry
     * @return {@link TestCompletedCheckpoint}
     */
    public static TestCompletedCheckpoint createCheckpoint(long id, SharedStateRegistry sharedStateRegistry) {

        int numberOfStates = 4;
        CheckpointProperties props = CheckpointProperties.forCheckpoint(CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION);

        OperatorID operatorID = new OperatorID();

        Map<OperatorID, OperatorState> operatorGroupState = new HashMap<>();
        OperatorState operatorState = new OperatorState(operatorID, numberOfStates, numberOfStates);
        operatorGroupState.put(operatorID, operatorState);

        for (int i = 0; i < numberOfStates; i++) {
            OperatorSubtaskState subtaskState = OperatorSubtaskState.builder().build();
            operatorState.putState(i, subtaskState);
        }

        operatorState.registerSharedStates(sharedStateRegistry, id);

        return new TestCompletedCheckpoint(new JobID(), id, 0, operatorGroupState, props);
    }

    protected static class TestCompletedCheckpoint extends CompletedCheckpoint {

        private static final long serialVersionUID = 4211419809665983026L;

        private boolean isDiscarded;

        // Latch for test variants which discard asynchronously
        private transient final CountDownLatch discardLatch = new CountDownLatch(1);

        public TestCompletedCheckpoint(
                JobID jobId,
                long checkpointId,
                long timestamp,
                Map<OperatorID, OperatorState> operatorGroupState,
                CheckpointProperties props) {

            super(jobId, checkpointId, timestamp, Long.MAX_VALUE, operatorGroupState, null, props,
                    new TestCompletedCheckpointStorageLocation(), null);
        }

        public boolean discardOnSubsume() throws Exception {
            if (super.shouldBeDiscardedOnSubsume()) {
                discard();
                return true;
            } else {
                return false;
            }
        }

        public boolean discardOnShutdown(JobStatus jobStatus) throws Exception {
            if (super.shouldBeDiscardedOnSubsume()) {
                discard();
                return true;
            } else {
                return false;
            }
        }

        public void discard() {
            if (!isDiscarded) {
                this.isDiscarded = true;

                if (discardLatch != null) {
                    discardLatch.countDown();
                }
            }
        }

        public boolean isDiscarded() {
            return isDiscarded;
        }

        public void awaitDiscard() throws InterruptedException {
            if (discardLatch != null) {
                discardLatch.await();
            }
        }

        public boolean awaitDiscard(long timeout) throws InterruptedException {
            if (discardLatch != null) {
                return discardLatch.await(timeout, TimeUnit.MILLISECONDS);
            } else {
                return false;
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            TestCompletedCheckpoint that = (TestCompletedCheckpoint) o;

            return getJobId().equals(that.getJobId())
                    && getCheckpointID() == that.getCheckpointID();
        }

        @Override
        public int hashCode() {
            return getJobId().hashCode() + (int) getCheckpointID();
        }
    }
}
