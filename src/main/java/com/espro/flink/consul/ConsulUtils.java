/*
 * Copyright (c) SABIO GmbH, Hamburg 2022 - All rights reserved
 */
package com.espro.flink.consul;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.flink.runtime.leaderelection.LeaderInformation;

/**
 * Utility class that provides useful methods to interact with Consul K/V store
 */
public final class ConsulUtils {

    private static final String LEADER_LATCH_NODE = "latch";

    private static final String CONNECTION_INFO_NODE = "connection_info";

    private ConsulUtils() {
        // utility class
    }

    /**
     * Converts the {@link LeaderInformation} into the byte representation for serialization purposes.
     *
     * @param leaderInformation converted to byte array
     * @return byte array representing the passed leader information
     * @throws IOException
     */
    public static byte[] leaderInformationToBytes(LeaderInformation leaderInformation) throws IOException {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final ObjectOutputStream oos = new ObjectOutputStream(baos);

        oos.writeUTF(leaderInformation.getLeaderAddress());
        oos.writeObject(leaderInformation.getLeaderSessionID());

        oos.close();

        return baos.toByteArray();
    }

    /**
     * Converts a byte array that represents {@link LeaderInformation} into an object.
     *
     * @param leaderInformationData byte array that is converted into {@link LeaderInformation}
     * @return {@link LeaderInformation}
     * @throws Exception
     */
    public static LeaderInformation bytesToLeaderInformation(byte[] leaderInformationData) throws Exception {
        try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(leaderInformationData))) {

            final String leaderAddress = ois.readUTF();
            final UUID leaderSessionID = (UUID) ois.readObject();
            return LeaderInformation.known(leaderSessionID, leaderAddress);
        }
    }

    /**
     * Creates a Consul path for the K/V store for storing leader information
     *
     * @param path specific component
     * @return path to key that holds information for latch
     */
    public static String generateConnectionInformationPath(String path) {
        return generateConsulPath(path, CONNECTION_INFO_NODE);
    }

    /**
     * Creates a Consul path for the K/V store for the leader election latch
     *
     * @param basePath - base path in consul k/v
     * @return path to key that holds information for latch
     */
    public static String getLeaderLatchPath(String basePath) {
        return generateConsulPath(basePath, LEADER_LATCH_NODE);
    }

    /**
     * Creates a Consul K/V path of the form "a/b/.../z".
     */
    public static String generateConsulPath(String... paths) {
        return Arrays.stream(paths)
                .map(ConsulUtils::trimSlashes)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.joining("/", "", ""));
    }

    private static String trimSlashes(String input) {
        int left = 0;
        int right = input.length() - 1;

        while (left <= right && input.charAt(left) == '/') {
            left++;
        }

        while (right >= left && input.charAt(right) == '/') {
            right--;
        }

        if (left <= right) {
            return input.substring(left, right + 1);
        } else {
            return "";
        }
    }
}
