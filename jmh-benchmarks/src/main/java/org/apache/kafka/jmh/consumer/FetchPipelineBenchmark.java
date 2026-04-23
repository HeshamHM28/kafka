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
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.GroupThreads;
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
 * Benchmarks the consumer fetch pipeline optimizations with proper old-vs-new
 * pairs for each optimization, both single-threaded and under contention.
 *
 * <h3>Optimization 1: fetchablePartitions + position() → fetchablePositions()</h3>
 * Old: fetchablePartitions() returns keys, then position() per partition (N+1 lock acquisitions).
 * New: fetchablePositions() returns positions in one pass (1 lock acquisition).
 *
 * <h3>Optimization 2: isAssigned + position → positionIfFetchable()</h3>
 * Old: isAssigned(tp) + position(tp) as separate synchronized calls (2 locks per partition).
 * New: positionIfFetchable(tp) combines both in a single synchronized call (1 lock per partition).
 *
 * <h3>Optimization 3: 3x tryUpdating* → tryUpdatingPartitionState()</h3>
 * Old: tryUpdatingHighWatermark + tryUpdatingLogStartOffset + tryUpdatingLastStableOffset
 *      as 3 separate synchronized calls (3 locks per partition).
 * New: tryUpdatingPartitionState() batches all updates in one call (1 lock per partition).
 */
@State(Scope.Group)
@Fork(value = 2)
@Warmup(iterations = 3, time = 5)
@Measurement(iterations = 5, time = 5)
@BenchmarkMode({Mode.Throughput, Mode.AverageTime})
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class FetchPipelineBenchmark {

    @Param({"100", "1000"})
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

    // =======================================================================
    // OPTIMIZATION 1: fetchablePartitions + position() → fetchablePositions()
    // =======================================================================

    // --- Single-threaded ---

    /** OLD: fetchablePartitions() then position() per partition. N+1 lock acquisitions. */
    @Benchmark
    @Group("opt1_single_old")
    @GroupThreads(1)
    public int opt1_old_fetchablePartitionsThenPosition() {
        List<TopicPartition> fetchable = subscriptionState.fetchablePartitions(tp -> true);
        int found = 0;
        for (TopicPartition tp : fetchable) {
            SubscriptionState.FetchPosition pos = subscriptionState.position(tp);
            if (pos != null) found++;
        }
        return found;
    }

    /** NEW: fetchablePositions() single pass. 1 lock acquisition. */
    @Benchmark
    @Group("opt1_single_new")
    @GroupThreads(1)
    public Map<TopicPartition, SubscriptionState.FetchPosition> opt1_new_fetchablePositions() {
        return subscriptionState.fetchablePositions(tp -> true);
    }

    // --- Contention: 2 readers + 1 writer ---

    @Benchmark
    @Group("opt1_contention_old")
    @GroupThreads(2)
    public int opt1_contention_old_read() {
        List<TopicPartition> fetchable = subscriptionState.fetchablePartitions(tp -> true);
        int found = 0;
        for (TopicPartition tp : fetchable) {
            SubscriptionState.FetchPosition pos = subscriptionState.position(tp);
            if (pos != null) found++;
        }
        return found;
    }

    @Benchmark
    @Group("opt1_contention_old")
    @GroupThreads(1)
    public int opt1_contention_old_write() {
        return updateWriter();
    }

    @Benchmark
    @Group("opt1_contention_new")
    @GroupThreads(2)
    public Map<TopicPartition, SubscriptionState.FetchPosition> opt1_contention_new_read() {
        return subscriptionState.fetchablePositions(tp -> true);
    }

    @Benchmark
    @Group("opt1_contention_new")
    @GroupThreads(1)
    public int opt1_contention_new_write() {
        return updateWriter();
    }

    // =======================================================================
    // OPTIMIZATION 2: isAssigned + position → positionIfFetchable()
    // =======================================================================

    // --- Single-threaded ---

    /** OLD: isAssigned(tp) + position(tp) separately. 2 lock acquisitions per partition. */
    @Benchmark
    @Group("opt2_single_old")
    @GroupThreads(1)
    public int opt2_old_isAssignedThenPosition() {
        int found = 0;
        for (TopicPartition tp : partitions) {
            if (subscriptionState.isAssigned(tp)) {
                SubscriptionState.FetchPosition pos = subscriptionState.position(tp);
                if (pos != null) found++;
            }
        }
        return found;
    }

    /** NEW: positionIfFetchable(tp) single call. 1 lock acquisition per partition. */
    @Benchmark
    @Group("opt2_single_new")
    @GroupThreads(1)
    public int opt2_new_positionIfFetchable() {
        int found = 0;
        for (TopicPartition tp : partitions) {
            SubscriptionState.FetchPosition pos = subscriptionState.positionIfFetchable(tp);
            if (pos != null) found++;
        }
        return found;
    }

    // --- Contention: 2 readers + 1 writer ---

    @Benchmark
    @Group("opt2_contention_old")
    @GroupThreads(2)
    public int opt2_contention_old_read() {
        int found = 0;
        for (TopicPartition tp : partitions) {
            if (subscriptionState.isAssigned(tp)) {
                SubscriptionState.FetchPosition pos = subscriptionState.position(tp);
                if (pos != null) found++;
            }
        }
        return found;
    }

    @Benchmark
    @Group("opt2_contention_old")
    @GroupThreads(1)
    public int opt2_contention_old_write() {
        return updateWriter();
    }

    @Benchmark
    @Group("opt2_contention_new")
    @GroupThreads(2)
    public int opt2_contention_new_read() {
        int found = 0;
        for (TopicPartition tp : partitions) {
            SubscriptionState.FetchPosition pos = subscriptionState.positionIfFetchable(tp);
            if (pos != null) found++;
        }
        return found;
    }

    @Benchmark
    @Group("opt2_contention_new")
    @GroupThreads(1)
    public int opt2_contention_new_write() {
        return updateWriter();
    }

    // =======================================================================
    // OPTIMIZATION 3: 3x tryUpdating* → tryUpdatingPartitionState()
    // =======================================================================

    // --- Single-threaded ---

    /** OLD: 3 separate tryUpdating* calls. 3 lock acquisitions per partition. */
    @Benchmark
    @Group("opt3_single_old")
    @GroupThreads(1)
    public int opt3_old_separateUpdates() {
        int updated = 0;
        for (TopicPartition tp : partitions) {
            subscriptionState.tryUpdatingHighWatermark(tp, 1000L);
            subscriptionState.tryUpdatingLogStartOffset(tp, 0L);
            if (subscriptionState.tryUpdatingLastStableOffset(tp, 999L))
                updated++;
        }
        return updated;
    }

    /** NEW: tryUpdatingPartitionState() batches all 3. 1 lock acquisition per partition. */
    @Benchmark
    @Group("opt3_single_new")
    @GroupThreads(1)
    public int opt3_new_batchedUpdate() {
        int updated = 0;
        for (TopicPartition tp : partitions) {
            if (subscriptionState.tryUpdatingPartitionState(tp, 1000L, 0L, 999L, -1, false, () -> 0L))
                updated++;
        }
        return updated;
    }

    // --- Contention: 2 readers + 1 writer ---

    @Benchmark
    @Group("opt3_contention_old")
    @GroupThreads(2)
    public int opt3_contention_old_read() {
        return subscriptionState.fetchablePartitions(tp -> true).size();
    }

    @Benchmark
    @Group("opt3_contention_old")
    @GroupThreads(1)
    public int opt3_contention_old_write() {
        int updated = 0;
        for (TopicPartition tp : partitions) {
            subscriptionState.tryUpdatingHighWatermark(tp, 1000L);
            subscriptionState.tryUpdatingLogStartOffset(tp, 0L);
            if (subscriptionState.tryUpdatingLastStableOffset(tp, 999L))
                updated++;
        }
        return updated;
    }

    @Benchmark
    @Group("opt3_contention_new")
    @GroupThreads(2)
    public int opt3_contention_new_read() {
        return subscriptionState.fetchablePartitions(tp -> true).size();
    }

    @Benchmark
    @Group("opt3_contention_new")
    @GroupThreads(1)
    public int opt3_contention_new_write() {
        int updated = 0;
        for (TopicPartition tp : partitions) {
            if (subscriptionState.tryUpdatingPartitionState(tp, 1000L, 0L, 999L, -1, false, () -> 0L))
                updated++;
        }
        return updated;
    }

    // =======================================================================
    // Shared writer workload for contention benchmarks
    // =======================================================================

    /** Uses tryUpdatingPartitionState to create realistic monitor contention. */
    private int updateWriter() {
        int updated = 0;
        for (TopicPartition tp : partitions) {
            if (subscriptionState.tryUpdatingPartitionState(tp, 1000L, 0L, 999L, -1, false, () -> 0L))
                updated++;
        }
        return updated;
    }
}
