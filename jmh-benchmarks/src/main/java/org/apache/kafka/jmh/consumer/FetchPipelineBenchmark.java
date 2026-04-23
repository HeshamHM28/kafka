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
 * Benchmarks the consumer fetch pipeline operations on SubscriptionState,
 * both single-threaded and under multi-threaded contention.
 *
 * <h3>Single-threaded benchmarks</h3>
 * Measure raw overhead of old vs new lookup patterns without contention.
 *
 * <h3>Multi-threaded contention benchmarks</h3>
 * Simulate the real consumer scenario: fetch threads read partition state
 * (fetchablePositions / fetchablePartitions+position) while other threads
 * concurrently write to the same synchronized monitor via position updates.
 *
 * <p>The key insight: the old read path acquires the monitor N+1 times
 * (1 for fetchablePartitions + N for position()), while the new path
 * acquires it once (fetchablePositions). Under contention, each monitor
 * acquisition is a point where a thread can be blocked by a writer,
 * so the old path suffers disproportionately.</p>
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
    private SubscriptionState.FetchPosition writePosition;

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
        // Position used by writer threads to create contention on the synchronized monitor
        writePosition = new SubscriptionState.FetchPosition(
            1L,
            Optional.of(1),
            new Metadata.LeaderAndEpoch(Optional.of(new Node(0, "host", 9092)), Optional.of(10))
        );
    }

    // -----------------------------------------------------------------------
    // Single-threaded benchmarks (no contention baseline)
    // -----------------------------------------------------------------------

    /**
     * Old pattern: fetchablePartitions() returns keys only, then position()
     * is called per partition. Each call acquires the synchronized monitor
     * and does a HashMap lookup.
     */
    @Benchmark
    @Group("singleOldFetchable")
    @GroupThreads(1)
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
     * New pattern: fetchablePositions() returns positions in a single
     * synchronized pass, eliminating per-partition re-lookups.
     */
    @Benchmark
    @Group("singleNewFetchable")
    @GroupThreads(1)
    public Map<TopicPartition, SubscriptionState.FetchPosition> testFetchablePositions() {
        return subscriptionState.fetchablePositions(tp -> true);
    }

    /**
     * Old pattern: per-partition isAssigned() + position() as separate
     * synchronized calls.
     */
    @Benchmark
    @Group("singleOldPerPartition")
    @GroupThreads(1)
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

    // -----------------------------------------------------------------------
    // Contention writer: updates positions to create realistic monitor
    // contention. Used by both old and new contention groups.
    // Each position(tp, pos) call acquires the synchronized monitor,
    // simulating fetch response handlers updating partition state.
    // -----------------------------------------------------------------------

    private int writerWorkload() {
        int updated = 0;
        for (TopicPartition tp : partitions) {
            subscriptionState.position(tp, writePosition);
            updated++;
        }
        return updated;
    }

    // -----------------------------------------------------------------------
    // Multi-threaded contention benchmarks — 2 readers + 1 writer
    // -----------------------------------------------------------------------

    /**
     * Old read path under contention: fetchablePartitions + per-partition position().
     * Acquires monitor N+1 times per operation — each acquisition is a contention point.
     */
    @Benchmark
    @Group("contentionOldFetchable")
    @GroupThreads(2)
    public int contention_oldFetchable_read() {
        List<TopicPartition> fetchable = subscriptionState.fetchablePartitions(tp -> true);
        int found = 0;
        for (TopicPartition tp : fetchable) {
            SubscriptionState.FetchPosition pos = subscriptionState.position(tp);
            if (pos != null) found++;
        }
        return found;
    }

    /**
     * Writer thread creating contention for old read path.
     */
    @Benchmark
    @Group("contentionOldFetchable")
    @GroupThreads(1)
    public int contention_oldFetchable_write() {
        return writerWorkload();
    }

    /**
     * New read path under contention: single fetchablePositions() call.
     * Acquires monitor once — minimal contention window.
     */
    @Benchmark
    @Group("contentionNewFetchable")
    @GroupThreads(2)
    public Map<TopicPartition, SubscriptionState.FetchPosition> contention_newFetchable_read() {
        return subscriptionState.fetchablePositions(tp -> true);
    }

    /**
     * Writer thread creating contention for new read path.
     */
    @Benchmark
    @Group("contentionNewFetchable")
    @GroupThreads(1)
    public int contention_newFetchable_write() {
        return writerWorkload();
    }

    // -----------------------------------------------------------------------
    // Heavy contention: 4 readers + 2 writers
    // -----------------------------------------------------------------------

    /**
     * Old read path under heavy contention (4 readers competing for the monitor).
     */
    @Benchmark
    @Group("heavyContentionOldFetchable")
    @GroupThreads(4)
    public int heavyContention_oldFetchable_read() {
        List<TopicPartition> fetchable = subscriptionState.fetchablePartitions(tp -> true);
        int found = 0;
        for (TopicPartition tp : fetchable) {
            SubscriptionState.FetchPosition pos = subscriptionState.position(tp);
            if (pos != null) found++;
        }
        return found;
    }

    /**
     * Writer threads under heavy contention for old read path.
     */
    @Benchmark
    @Group("heavyContentionOldFetchable")
    @GroupThreads(2)
    public int heavyContention_oldFetchable_write() {
        return writerWorkload();
    }

    /**
     * New read path under heavy contention (4 readers).
     */
    @Benchmark
    @Group("heavyContentionNewFetchable")
    @GroupThreads(4)
    public Map<TopicPartition, SubscriptionState.FetchPosition> heavyContention_newFetchable_read() {
        return subscriptionState.fetchablePositions(tp -> true);
    }

    /**
     * Writer threads under heavy contention for new read path.
     */
    @Benchmark
    @Group("heavyContentionNewFetchable")
    @GroupThreads(2)
    public int heavyContention_newFetchable_write() {
        return writerWorkload();
    }
}
