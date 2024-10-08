/*
 * Copyright (c) SABIO GmbH, Hamburg 2021 - All rights reserved
 */
package com.espro.flink.consul;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.persistence.IntegerResourceVersion;
import org.apache.flink.runtime.persistence.RetrievableStateStorageHelper;
import org.apache.flink.runtime.persistence.filesystem.FileSystemStateStorageHelper;
import org.apache.flink.runtime.state.RetrievableStateHandle;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.runtime.state.SharedStateRegistryImpl;
import org.apache.flink.shaded.curator5.org.apache.curator.shaded.com.google.common.io.Files;
import org.junit.Before;
import org.junit.Test;

import com.ecwid.consul.v1.ConsulClient;
import com.espro.flink.consul.checkpoint.CheckpointTestHelper;

/**
 * Tests the {@link ConsulStateHandleStore}.
 */
public class ConsulStateHandleStoreTest extends AbstractConsulTest {

    private static final String BASE_PATH = "flink/states/";

    private static final String STORAGE_PREFIX = "sp";

    private ConsulClientProvider clientProviderImpl;
    private File tempDir;
    private RetrievableStateStorageHelper<CompletedCheckpoint> storage;

    private static AtomicLong checkpointIdCounter = new AtomicLong();

    @Before
    public void setup() throws IOException {
        clientProviderImpl = new ConsulClientProviderImpl(new ConsulClient("localhost", consul.getHttpPort()));
        tempDir = Files.createTempDir();
        storage = new FileSystemStateStorageHelper<>(new Path(tempDir.getPath()), STORAGE_PREFIX);
    }

    @Test
    public void testgetAndLock() throws Exception {
        // GIVEN shared registry and job id
        SharedStateRegistry sharedStateRegistry = new SharedStateRegistryImpl();
        JobID jobID = JobID.generate();

        // GIVEN ConsulStateHandleStore
        ConsulStateHandleStore<CompletedCheckpoint> store = new ConsulStateHandleStore<>(clientProviderImpl, storage, BASE_PATH);

        // GIVEN checkpoint
        CompletedCheckpoint checkpoint = addCheckpoint(sharedStateRegistry, jobID, store);

        // WHEN getting checkpoint by path
        RetrievableStateHandle<CompletedCheckpoint> stateHandle = store.getAndLock(createCheckpointKey(jobID, checkpoint));

        // THEN got state handle is not null
        assertNotNull(stateHandle);
    }

    @Test
    public void testGetAllAndLock() throws Exception {
        // GIVEN shared registry and job id
        SharedStateRegistry sharedStateRegistry = new SharedStateRegistryImpl();
        JobID jobID = JobID.generate();

        // GIVEN ConsulStateHandleStore
        ConsulStateHandleStore<CompletedCheckpoint> store = new ConsulStateHandleStore<>(clientProviderImpl, storage, BASE_PATH);

        // GIVEN 10 checkpoints
        Set<CompletedCheckpoint> checkpoints = addCheckpoints(10, sharedStateRegistry, jobID, store);

        // WHEN getting all state handle tuples from ConsulStateHandleStore
        List<Tuple2<RetrievableStateHandle<CompletedCheckpoint>, String>> stateHandleTuples = store.getAllAndLock();

        // THEN number of handles is equal to number of created checkpoints
        assertEquals(checkpoints.size(), stateHandleTuples.size());

        // THEN tuples for all created checkpoint are returned
        Set<String> expectedCheckpointPaths = checkpoints.stream().map(c -> BASE_PATH + getCheckpointPath(jobID, c))
                .collect(Collectors.toSet());
        for (Tuple2<RetrievableStateHandle<CompletedCheckpoint>, String> tuple : stateHandleTuples) {
            assertTrue(expectedCheckpointPaths.contains(tuple.f1));
            assertNotNull(tuple.f0);
        }
    }

    @Test
    public void testGetAllAndLock_NoHandlesPresent() throws Exception {
        // GIVEN ConsulStateHandleStore
        ConsulStateHandleStore<CompletedCheckpoint> store = new ConsulStateHandleStore<>(clientProviderImpl, storage, "foo");

        // WHEN getting all state handle tuples from ConsulStateHandleStore
        List<Tuple2<RetrievableStateHandle<CompletedCheckpoint>, String>> stateHandleTuples = store.getAllAndLock();

        // THEN no state handle tuples are present
        assertTrue(stateHandleTuples.isEmpty());
    }

    @Test
    public void testGetAllHandles() throws Exception {
        // GIVEN shared registry and job id
        SharedStateRegistry sharedStateRegistry = new SharedStateRegistryImpl();
        JobID jobID = JobID.generate();

        // GIVEN ConsulStateHandleStore
        ConsulStateHandleStore<CompletedCheckpoint> store = new ConsulStateHandleStore<>(clientProviderImpl, storage, BASE_PATH);

        // GIVEN 10 checkpoints
        Set<CompletedCheckpoint> checkpoints = addCheckpoints(10, sharedStateRegistry, jobID, store);

        // WHEN getting all handles from ConsulStateHandleStore
        Collection<String> allHandles = store.getAllHandles();

        // THEN number of handles is equal to number of created checkpoints
        assertEquals(checkpoints.size(), allHandles.size());

        // THEN all created checkpoint handles are returned
        for (CompletedCheckpoint completedCheckpoint : checkpoints) {
            String expectedCheckpointHandle = getCheckpointPath(jobID, completedCheckpoint);
            assertTrue(allHandles.contains(expectedCheckpointHandle));
        }
    }

    @Test
    public void testGetAllHandles_NoHandlesPresent() throws Exception {
        // GIVEN ConsulStateHandleStore
        ConsulStateHandleStore<CompletedCheckpoint> store = new ConsulStateHandleStore<>(clientProviderImpl, storage, "foo");

        // WHEN getting all handles from ConsulStateHandleStore
        Collection<String> allHandles = store.getAllHandles();

        // THEN no handles are present
        assertTrue(allHandles.isEmpty());
    }

    @Test
    public void testReleaseAndTryRemove() throws Exception {
        // GIVEN shared registry and job id
        SharedStateRegistry sharedStateRegistry = new SharedStateRegistryImpl();
        JobID jobID = JobID.generate();

        // GIVEN ConsulStateHandleStore
        ConsulStateHandleStore<CompletedCheckpoint> store = new ConsulStateHandleStore<>(clientProviderImpl, storage, BASE_PATH);

        // GIVEN checkpoint
        CompletedCheckpoint checkpoint = addCheckpoint(sharedStateRegistry, jobID, store);

        // WHEN getting checkpoint by path
        boolean success = store.releaseAndTryRemove(getCheckpointPath(jobID, checkpoint));

        // THEN removal was successful
        assertTrue(success);

        // THEN state in Consul is deleted
        assertNull(clientProviderImpl.get().getKVBinaryValue(getCheckpointPath(jobID, checkpoint)).getValue());
    }

    @Test
    public void testExists() throws Exception {
        // GIVEN shared registry and job id
        SharedStateRegistry sharedStateRegistry = new SharedStateRegistryImpl();
        JobID jobID = JobID.generate();

        // GIVEN ConsulStateHandleStore
        ConsulStateHandleStore<CompletedCheckpoint> store = new ConsulStateHandleStore<>(clientProviderImpl, storage, BASE_PATH);

        // GIVEN checkpoint
        CompletedCheckpoint checkpoint = addCheckpoint(sharedStateRegistry, jobID, store);

        // WHEN checking if the path of the created checkpoint exists
        IntegerResourceVersion resourceVersion = store.exists(createCheckpointKey(jobID, checkpoint));

        // THEN resource version is equal to modify index of Consul value
        assertEquals(clientProviderImpl.get().getKVBinaryValue(BASE_PATH + getCheckpointPath(jobID, checkpoint)).getValue().getModifyIndex(),
                resourceVersion.getValue());
    }

    private static Set<CompletedCheckpoint> addCheckpoints(int numberOfCheckpoints, SharedStateRegistry sharedStateRegistry, JobID jobID,
            ConsulStateHandleStore<CompletedCheckpoint> store) throws Exception {
        Set<CompletedCheckpoint> checkpoints = new HashSet<>();
        for (int i = 0; i < numberOfCheckpoints; i++) {
            checkpoints.add(addCheckpoint(sharedStateRegistry, jobID, store));
        }
        return checkpoints;
    }

    private static CompletedCheckpoint addCheckpoint(SharedStateRegistry sharedStateRegistry, JobID jobID,
            ConsulStateHandleStore<CompletedCheckpoint> store) throws Exception {
        CompletedCheckpoint checkpoint = CheckpointTestHelper.createCheckpoint(checkpointIdCounter.incrementAndGet(), sharedStateRegistry);
        store.addAndLock(createCheckpointKey(jobID, checkpoint), checkpoint);
        return checkpoint;
    }

    private static String getCheckpointPath(JobID jobID, CompletedCheckpoint checkpoint2) {
        return jobID.toString() + checkpoint2.getCheckpointID();
    }

    private static String createCheckpointKey(JobID jobID, CompletedCheckpoint checkpoint2) {
        return BASE_PATH + getCheckpointPath(jobID, checkpoint2);
    }
}
