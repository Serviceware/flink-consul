/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.espro.flink.consul.leader;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.UUID;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.flink.util.Preconditions;

/**
 * POJO that is used to write to Consul. Holds information about the current leadership.
 */
final class ConsulLeaderData {

    private final UUID leaderIdentifier;

    private ConsulLeaderData(UUID leaderIdentifier) {
        this.leaderIdentifier = Preconditions.checkNotNull(leaderIdentifier, "sessionId");
	}

    public static ConsulLeaderData from(UUID leaderIdentifier) {
        return new ConsulLeaderData(leaderIdentifier);
	}

	public static ConsulLeaderData from(byte[] bytes) {
		try {
			ByteArrayInputStream is = new ByteArrayInputStream(bytes);
			ObjectInputStream ois = new ObjectInputStream(is);
            UUID leaderIdentifier = (UUID) ois.readObject();
            return from(leaderIdentifier);
		} catch (IOException | ClassNotFoundException e) {
			throw new IllegalArgumentException("ConsulLeaderData deserialization failure", e);
		}
	}

	public byte[] toBytes() {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try {
			ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(leaderIdentifier);
			return baos.toByteArray();
		} catch (IOException e) {
			throw new IllegalStateException("ConsulLeaderData serialization failure", e);
		}
	}

    public UUID getLeaderIdentifier() {
        return leaderIdentifier;
	}

	@Override
	public int hashCode() {
		return HashCodeBuilder.reflectionHashCode(this);
	}

	@Override
	public boolean equals(Object obj) {
		return EqualsBuilder.reflectionEquals(this, obj);
	}

	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this, ToStringStyle.SIMPLE_STYLE);
	}
}
