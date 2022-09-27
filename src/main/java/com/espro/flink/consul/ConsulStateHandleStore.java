/*
 * Copyright (c) SABIO GmbH, Hamburg 2021 - All rights reserved
 */
package com.espro.flink.consul;

import static java.util.Collections.emptyList;

import static org.apache.commons.lang3.StringUtils.removeEnd;
import static org.apache.flink.util.Preconditions.checkNotNull;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.persistence.IntegerResourceVersion;
import org.apache.flink.runtime.persistence.RetrievableStateStorageHelper;
import org.apache.flink.runtime.persistence.StateHandleStore;
import org.apache.flink.runtime.state.RetrievableStateHandle;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.InstantiationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.kv.model.GetBinaryValue;

/**
 * Class which stores state via the provided {@link RetrievableStateStorageHelper} and writes the returned state handle to Consul.
 * <p>
 * Added state is persisted via {@link RetrievableStateHandle RetrievableStateHandles}, which in turn are written to Consul. This level of
 * indirection is necessary to keep the amount of data in Consul small. Consul is build for data to be not larger than 512 KB whereas state
 * can grow to multiple MBs.
 */
public class ConsulStateHandleStore<T extends Serializable> implements StateHandleStore<T, IntegerResourceVersion> {

    private static final Logger LOG = LoggerFactory.getLogger(ConsulStateHandleStore.class);

    private final Supplier<ConsulClient> clientProvider;

    private final RetrievableStateStorageHelper<T> storage;

    private final String basePathInConsul;

    public ConsulStateHandleStore(Supplier<ConsulClient> clientProvider, RetrievableStateStorageHelper<T> storage,
            String basePathInConsul) {
        this.clientProvider = clientProvider;
        this.storage = storage;
        this.basePathInConsul = removeEnd(basePathInConsul, "/");
    }

    @Override
    public RetrievableStateHandle<T> addAndLock(String keyName, T state) throws Exception {
        checkNotNull(keyName, "Name of key in Consul");
        checkNotNull(state, "State");

        String fullConsulPathToKey = fullPathToKey(keyName);
        LOG.debug("Add state to consul key/value store {}", fullConsulPathToKey);

        RetrievableStateHandle<T> storeHandle = storage.store(state);
        boolean success = false;

        try {
            byte[] serializedStoreHandle = InstantiationUtil.serializeObject(storeHandle);
            success = clientProvider.get().setKVBinaryValue(fullConsulPathToKey, serializedStoreHandle).getValue();
            return storeHandle;
        } finally {
            if (!success) {
                // cleanup if data was not stored in Consul
                if (storeHandle != null) {
                    storeHandle.discardState();
                }
            }
        }
    }

    @Override
    public void replace(String keyName, IntegerResourceVersion resourceVersion, T state) throws Exception {
        checkNotNull(keyName, "Name of key in Consul");
        checkNotNull(state, "State");

        String fullConsulPathToKey = fullPathToKey(keyName);
        LOG.debug("Replace state in consul key/value store {}", fullConsulPathToKey);

        RetrievableStateHandle<T> oldStateHandle = get(fullConsulPathToKey);
        RetrievableStateHandle<T> newStateHandle = storage.store(state);

        boolean success = false;

        try {
            byte[] serializedStoreHandle = InstantiationUtil.serializeObject(newStateHandle);
            success = clientProvider.get().setKVBinaryValue(fullConsulPathToKey, serializedStoreHandle).getValue();
        } finally {
            if (success) {
                oldStateHandle.discardState();
            } else {
                newStateHandle.discardState();
            }
        }
    }

    @Override
    public IntegerResourceVersion exists(String keyName) throws Exception {
        checkNotNull(keyName, "Name of key in Consul");

        String fullConsulPathToKey = fullPathToKey(keyName);

        GetBinaryValue binaryValue = clientProvider.get().getKVBinaryValue(fullConsulPathToKey).getValue();
        if (binaryValue != null) {
            return IntegerResourceVersion.valueOf((int) binaryValue.getModifyIndex());
        } else {
            return IntegerResourceVersion.notExisting();
        }
    }

    @Override
    public RetrievableStateHandle<T> getAndLock(String keyName) throws Exception {
        checkNotNull(keyName, "Name of key in Consul");
        String fullConsulPathToKey = fullPathToKey(keyName);
        return get(fullConsulPathToKey);
    }

    @Override
    public List<Tuple2<RetrievableStateHandle<T>, String>> getAllAndLock() throws Exception {
        List<Tuple2<RetrievableStateHandle<T>, String>> stateHandles = new ArrayList<>();

        List<GetBinaryValue> binaryValues = clientProvider.get().getKVBinaryValues(basePathInConsul).getValue();
        if (binaryValues == null || binaryValues.isEmpty()) {
            LOG.debug("No state handles present in Consul for key prefix {}", basePathInConsul);
            return Collections.emptyList();
        }

        for (GetBinaryValue binaryValue : binaryValues) {
            RetrievableStateHandle<T> stateHandle = InstantiationUtil.<RetrievableStateHandle<T>>deserializeObject(
                    binaryValue.getValue(),
                    Thread.currentThread().getContextClassLoader());

            stateHandles.add(new Tuple2<>(stateHandle, binaryValue.getKey()));
        }
        return stateHandles;
    }

    @Override
    public Collection<String> getAllHandles() throws Exception {
        List<String> keys = clientProvider.get().getKVKeysOnly(basePathInConsul).getValue();
        if (keys != null) {
            return keys.stream()
                    // Remove base path
                    .map(key -> StringUtils.removeStart(key, basePathInConsul))
                    // Remove leading slashes
                    .map(key -> StringUtils.removeStart(key, "/"))
                    .collect(Collectors.toList());
        }

        LOG.debug("No handles present in Consul for key prefix {}", basePathInConsul);
        return emptyList();
    }

    @Override
    public boolean releaseAndTryRemove(String keyName) throws Exception {
        checkNotNull(keyName, "Name of key in Consul");

        String fullConsulPathToKey = fullPathToKey(keyName);
        LOG.debug("Remove state in consul key/value store {}", fullConsulPathToKey);

        RetrievableStateHandle<T> stateHandle = null;
        try {
            stateHandle = get(fullConsulPathToKey);
        } catch (Exception e) {
            LOG.warn("Could not retrieve the state handle from Consul {}.", fullConsulPathToKey, e);
        }

        try {
            clientProvider.get().deleteKVValue(fullConsulPathToKey);
            LOG.info("Value for key {} in Consul was deleted.", fullConsulPathToKey);
        } catch (Exception e) {
            LOG.info("Error while deleting state handle for path {}", fullConsulPathToKey, e);
            return false;
        }

        if (stateHandle != null) {
            stateHandle.discardState();
        }

        return true;
    }

    @Override
    public void releaseAndTryRemoveAll() throws Exception {
        Collection<String> allPathsInConsul = getAllHandles();

        Exception exception = null;

        for (String pathInConsul : allPathsInConsul) {
            try {
                releaseAndTryRemove(pathInConsul);
            } catch (Exception e) {
                exception = ExceptionUtils.firstOrSuppressed(e, exception);
            }
        }

        if (exception != null) {
            throw new Exception("Could not properly release and try removing all states.", exception);
        }
    }

    @Override
    public void clearEntries() throws Exception {
        Collection<String> allPathsInConsul = getAllHandles();
        for (String pathInConsul : allPathsInConsul) {
            clientProvider.get().deleteKVValue(pathInConsul);
            LOG.info("Value for key {} in Consul was deleted.", pathInConsul);
        }
    }

    @Override
    public void release(String pathInConsul) throws Exception {
        // There is no dedicated lock for a specific state handle
    }

    @Override
    public void releaseAll() throws Exception {
        // There are no locks
    }

    private RetrievableStateHandle<T> get(String path) {
        try {
            GetBinaryValue binaryValue = clientProvider.get().getKVBinaryValue(path).getValue();

            return InstantiationUtil.<RetrievableStateHandle<T>>deserializeObject(
                    binaryValue.getValue(),
                    Thread.currentThread().getContextClassLoader());
        } catch (IOException | ClassNotFoundException e) {
            throw new IllegalStateException(e);
        }
    }

    private String fullPathToKey(String keyName) {
        return basePathInConsul + normalizePath(keyName);
    }

    /**
     * Makes sure that every path starts with a "/".
     *
     * @param path Path to normalize
     * @return Normalized path such that it starts with a "/"
     */
    private static String normalizePath(String path) {
        if (path.startsWith("/")) {
            return path;
        } else {
            return '/' + path;
        }
    }
}
