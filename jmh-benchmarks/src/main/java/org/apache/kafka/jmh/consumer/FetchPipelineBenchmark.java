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
 * Benchmarks the consumer fetch pipeline operations on SubscriptionState.
 *
 * The key optimization is fetchablePositions() which returns partition positions in a
 * single synchronized pass, replacing the old pattern of fetchablePartitions() (which
 * discarded state) followed by per-partition position() re-lookups.
 *
 * Also benchmarks per-partition isAssigned+position lookups which the batch methods
 * in FetchCollector now consolidate into single synchronized calls.
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
     * Simulates the pre-optimization prepareFetchRequests() pattern:
     * fetchablePartitions() returns keys only, then position() is called per partition.
     * Each call acquires the synchronized monitor and does a HashMap lookup.
     */
    @Benchmark
    public int testFetchablePartitionsThenPositionLookup() {
        List<TopicPartition> fetchable = subscriptionState.fetchablePartitions(tp -> true);
        int found = 0;
        for (TopicPartition tp : fetchable) {
            SubscriptionState.FetchPosition pos = subscriptionState.position(tp);
            if (pos != null) found++;
        }
        return found;
    }

    /**
     * The optimized path: fetchablePositions() returns positions in a single synchronized
     * pass, eliminating per-partition re-lookups.
     */
    @Benchmark
    public Map<TopicPartition, SubscriptionState.FetchPosition> testFetchablePositions() {
        return subscriptionState.fetchablePositions(tp -> true);
    }

    /**
     * Simulates the pre-optimization fetchRecords() pattern per partition:
     * isAssigned() + position() as separate synchronized calls.
     */
    @Benchmark
    public int testPerPartitionAssignedAndPosition() {
        int found = 0;
        for (TopicPartition tp : partitions) {
            if (subscriptionState.isAssigned(tp)) {
                SubscriptionState.FetchPosition pos = subscriptionState.position(tp);
                if (pos != null) found++;
            }
        }
        return found;
    }
}
