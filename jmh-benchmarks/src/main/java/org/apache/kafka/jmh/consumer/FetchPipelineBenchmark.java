/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.jmh.consumer;

import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.consumer.internals.AutoOffsetResetStrategy;
import org.apache.kafka.clients.consumer.internals.SubscriptionState;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.LogContext;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Benchmarks the consumer fetch pipeline hot-path operations on SubscriptionState.
 * These methods are called per-partition on every fetch cycle and fetch response.
 */
@State(Scope.Benchmark)
@Fork(value = 2)
@Warmup(iterations = 3, time = 5)
@Measurement(iterations = 5, time = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class FetchPipelineBenchmark {

    @Param({"100", "1000", "5000"})
    int partitionCount;

    private SubscriptionState subscriptionState;
    private List<TopicPartition> partitions;

    @Setup(Level.Trial)
    public void setup() {
        Set<TopicPartition> assignment = new HashSet<>(partitionCount);
        partitions = new ArrayList<>(partitionCount);
        for (int i = 0; i < partitionCount; i++) {
            TopicPartition tp = new TopicPartition("topic-" + (i / 50), i % 50);
            assignment.add(tp);
            partitions.add(tp);
        }
        subscriptionState = new SubscriptionState(new LogContext(), AutoOffsetResetStrategy.EARLIEST);
        subscriptionState.assignFromUser(assignment);
        SubscriptionState.FetchPosition position = new SubscriptionState.FetchPosition(
            0L,
            Optional.of(0),
            new Metadata.LeaderAndEpoch(Optional.of(new Node(0, "host", 9092)), Optional.of(10))
        );
        for (TopicPartition tp : assignment) {
            subscriptionState.seekUnvalidated(tp, position);
            subscriptionState.completeValidation(tp);
        }
    }

    /**
     * Benchmarks the atomic positionIfFetchable() which combines isAssigned + isFetchable + position
     * into a single synchronized lookup. Called per-partition in FetchCollector.fetchRecords().
     */
    @Benchmark
    public int testPositionIfFetchable() {
        int found = 0;
        for (TopicPartition tp : partitions) {
            if (subscriptionState.positionIfFetchable(tp) != null) {
                found++;
            }
        }
        return found;
    }

    /**
     * Benchmarks the batch partition state update which combines highWatermark + logStartOffset +
     * lastStableOffset + preferredReadReplica updates into a single synchronized call.
     * Called per-partition in FetchCollector.updatePartitionState().
     */
    @Benchmark
    public int testTryUpdatingPartitionState() {
        int updated = 0;
        for (TopicPartition tp : partitions) {
            if (subscriptionState.tryUpdatingPartitionState(tp, 100L, 0L, 90L, -1, false, null)) {
                updated++;
            }
        }
        return updated;
    }

    /**
     * Benchmarks fetchablePositions() which returns positions for all fetchable partitions
     * in a single synchronized pass. Called once per fetch cycle in AbstractFetch.prepareFetchRequests().
     */
    @Benchmark
    public Map<TopicPartition, SubscriptionState.FetchPosition> testFetchablePositions() {
        return subscriptionState.fetchablePositions(tp -> true);
    }
}
