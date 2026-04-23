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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Stress test for the optimized SubscriptionState fetch pipeline methods.
 *
 * Validates correctness under heavy concurrent access:
 * - No exceptions or deadlocks under sustained load
 * - Data consistency (positions are never null for assigned partitions)
 * - All 3 optimized paths work correctly under contention
 *
 * Run directly: java -cp kafka-jmh-benchmarks-*-all.jar \
 *     org.apache.kafka.jmh.consumer.FetchPipelineStressTest
 */
public class FetchPipelineStressTest {

    private static final int PARTITION_COUNT = 500;
    private static final int DURATION_SECONDS = 30;

    // Thread counts for different roles
    private static final int FETCH_POSITION_READERS = 4;   // fetchablePositions() readers
    private static final int PER_PARTITION_READERS = 4;     // positionIfFetchable() readers
    private static final int BATCH_WRITERS = 2;             // tryUpdatingPartitionState() writers
    private static final int INDIVIDUAL_WRITERS = 2;        // 3x tryUpdating* writers
    private static final int POSITION_WRITERS = 2;          // position(tp, pos) writers

    private static final int TOTAL_THREADS = FETCH_POSITION_READERS + PER_PARTITION_READERS
            + BATCH_WRITERS + INDIVIDUAL_WRITERS + POSITION_WRITERS;

    public static void main(String[] args) throws Exception {
        System.out.println("=== FetchPipeline Stress Test ===");
        System.out.printf("Partitions: %d | Threads: %d | Duration: %ds%n",
                PARTITION_COUNT, TOTAL_THREADS, DURATION_SECONDS);
        System.out.println();

        // Setup
        Set<TopicPartition> assignment = new HashSet<>(PARTITION_COUNT);
        List<TopicPartition> partitions = new ArrayList<>(PARTITION_COUNT);
        for (int i = 0; i < PARTITION_COUNT; i++) {
            TopicPartition tp = new TopicPartition("topic-" + (i / 50), i % 50);
            assignment.add(tp);
            partitions.add(tp);
        }

        SubscriptionState subscriptionState = new SubscriptionState(
                new LogContext(), AutoOffsetResetStrategy.EARLIEST);
        subscriptionState.assignFromUser(assignment);

        SubscriptionState.FetchPosition initPosition = new SubscriptionState.FetchPosition(
                0L, Optional.of(0),
                new Metadata.LeaderAndEpoch(Optional.of(new Node(0, "host", 9092)), Optional.of(10)));

        for (TopicPartition tp : assignment) {
            subscriptionState.seekUnvalidated(tp, initPosition);
            subscriptionState.completeValidation(tp);
        }

        // Counters
        AtomicLong fetchPositionsOps = new AtomicLong();
        AtomicLong perPartitionOps = new AtomicLong();
        AtomicLong batchWriteOps = new AtomicLong();
        AtomicLong individualWriteOps = new AtomicLong();
        AtomicLong positionWriteOps = new AtomicLong();
        AtomicInteger errors = new AtomicInteger();
        AtomicInteger consistencyErrors = new AtomicInteger();
        AtomicReference<Throwable> firstError = new AtomicReference<>();

        // Synchronization
        CyclicBarrier startBarrier = new CyclicBarrier(TOTAL_THREADS);
        CountDownLatch doneLatch = new CountDownLatch(TOTAL_THREADS);
        AtomicBoolean running = new AtomicBoolean(true);

        ExecutorService executor = Executors.newFixedThreadPool(TOTAL_THREADS);

        // --- fetchablePositions() readers ---
        for (int t = 0; t < FETCH_POSITION_READERS; t++) {
            executor.submit(() -> {
                try {
                    startBarrier.await();
                    while (running.get()) {
                        Map<TopicPartition, SubscriptionState.FetchPosition> positions =
                                subscriptionState.fetchablePositions(tp -> true);
                        // Consistency check: all returned positions must be non-null
                        for (Map.Entry<TopicPartition, SubscriptionState.FetchPosition> entry : positions.entrySet()) {
                            if (entry.getValue() == null) {
                                consistencyErrors.incrementAndGet();
                                System.err.printf("CONSISTENCY ERROR: fetchablePositions returned null for %s%n",
                                        entry.getKey());
                            }
                        }
                        fetchPositionsOps.incrementAndGet();
                    }
                } catch (Throwable e) {
                    errors.incrementAndGet();
                    firstError.compareAndSet(null, e);
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        // --- positionIfFetchable() readers ---
        for (int t = 0; t < PER_PARTITION_READERS; t++) {
            executor.submit(() -> {
                try {
                    startBarrier.await();
                    while (running.get()) {
                        int found = 0;
                        for (TopicPartition tp : partitions) {
                            SubscriptionState.FetchPosition pos =
                                    subscriptionState.positionIfFetchable(tp);
                            if (pos != null) found++;
                        }
                        // Consistency check: all partitions are assigned and fetchable
                        if (found != PARTITION_COUNT) {
                            consistencyErrors.incrementAndGet();
                            System.err.printf("CONSISTENCY ERROR: positionIfFetchable found %d/%d%n",
                                    found, PARTITION_COUNT);
                        }
                        perPartitionOps.incrementAndGet();
                    }
                } catch (Throwable e) {
                    errors.incrementAndGet();
                    firstError.compareAndSet(null, e);
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        // --- tryUpdatingPartitionState() batch writers ---
        for (int t = 0; t < BATCH_WRITERS; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    startBarrier.await();
                    long iteration = 0;
                    while (running.get()) {
                        long watermark = 1000 + iteration + threadId;
                        for (TopicPartition tp : partitions) {
                            boolean updated = subscriptionState.tryUpdatingPartitionState(
                                    tp, watermark, iteration, watermark - 1,
                                    -1, false, () -> 0L);
                            if (!updated) {
                                consistencyErrors.incrementAndGet();
                                System.err.printf("CONSISTENCY ERROR: tryUpdatingPartitionState returned false for %s%n", tp);
                            }
                        }
                        iteration++;
                        batchWriteOps.incrementAndGet();
                    }
                } catch (Throwable e) {
                    errors.incrementAndGet();
                    firstError.compareAndSet(null, e);
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        // --- Individual tryUpdating* writers (old pattern) ---
        for (int t = 0; t < INDIVIDUAL_WRITERS; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    startBarrier.await();
                    long iteration = 0;
                    while (running.get()) {
                        long watermark = 2000 + iteration + threadId;
                        for (TopicPartition tp : partitions) {
                            subscriptionState.tryUpdatingHighWatermark(tp, watermark);
                            subscriptionState.tryUpdatingLogStartOffset(tp, iteration);
                            subscriptionState.tryUpdatingLastStableOffset(tp, watermark - 1);
                        }
                        iteration++;
                        individualWriteOps.incrementAndGet();
                    }
                } catch (Throwable e) {
                    errors.incrementAndGet();
                    firstError.compareAndSet(null, e);
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        // --- position() writers (seek operations) ---
        for (int t = 0; t < POSITION_WRITERS; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    startBarrier.await();
                    long iteration = 0;
                    while (running.get()) {
                        SubscriptionState.FetchPosition pos = new SubscriptionState.FetchPosition(
                                iteration + threadId, Optional.of((int) (iteration % 100)),
                                new Metadata.LeaderAndEpoch(
                                        Optional.of(new Node(0, "host", 9092)),
                                        Optional.of(10)));
                        for (TopicPartition tp : partitions) {
                            subscriptionState.position(tp, pos);
                        }
                        iteration++;
                        positionWriteOps.incrementAndGet();
                    }
                } catch (Throwable e) {
                    errors.incrementAndGet();
                    firstError.compareAndSet(null, e);
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        // Progress reporting
        System.out.printf("%-6s  %12s  %12s  %12s  %12s  %12s  %6s  %6s%n",
                "Time", "fetchPos/s", "perPart/s", "batchWr/s", "indivWr/s", "posWr/s", "Errors", "Consist");

        long startTime = System.currentTimeMillis();
        long prevFetchPos = 0, prevPerPart = 0, prevBatchWr = 0, prevIndivWr = 0, prevPosWr = 0;

        for (int sec = 1; sec <= DURATION_SECONDS; sec++) {
            Thread.sleep(1000);
            long curFetchPos = fetchPositionsOps.get();
            long curPerPart = perPartitionOps.get();
            long curBatchWr = batchWriteOps.get();
            long curIndivWr = individualWriteOps.get();
            long curPosWr = positionWriteOps.get();

            System.out.printf("%-6ds  %12d  %12d  %12d  %12d  %12d  %6d  %6d%n",
                    sec,
                    curFetchPos - prevFetchPos,
                    curPerPart - prevPerPart,
                    curBatchWr - prevBatchWr,
                    curIndivWr - prevIndivWr,
                    curPosWr - prevPosWr,
                    errors.get(),
                    consistencyErrors.get());

            prevFetchPos = curFetchPos;
            prevPerPart = curPerPart;
            prevBatchWr = curBatchWr;
            prevIndivWr = curIndivWr;
            prevPosWr = curPosWr;
        }

        // Stop
        running.set(false);
        boolean allDone = doneLatch.await(10, TimeUnit.SECONDS);
        executor.shutdownNow();

        long elapsed = System.currentTimeMillis() - startTime;

        // Summary
        System.out.println();
        System.out.println("=== RESULTS ===");
        System.out.printf("Duration:              %d ms%n", elapsed);
        System.out.printf("All threads finished:  %s%n", allDone ? "YES" : "NO (possible deadlock!)");
        System.out.println();
        System.out.println("Total operations:");
        System.out.printf("  fetchablePositions reads:   %,d  (%,d ops/s)%n",
                fetchPositionsOps.get(), fetchPositionsOps.get() * 1000 / elapsed);
        System.out.printf("  positionIfFetchable reads:  %,d  (%,d ops/s)%n",
                perPartitionOps.get(), perPartitionOps.get() * 1000 / elapsed);
        System.out.printf("  Batched writes:             %,d  (%,d ops/s)%n",
                batchWriteOps.get(), batchWriteOps.get() * 1000 / elapsed);
        System.out.printf("  Individual writes:          %,d  (%,d ops/s)%n",
                individualWriteOps.get(), individualWriteOps.get() * 1000 / elapsed);
        System.out.printf("  Position writes:            %,d  (%,d ops/s)%n",
                positionWriteOps.get(), positionWriteOps.get() * 1000 / elapsed);
        System.out.println();
        System.out.printf("Errors:                %d%n", errors.get());
        System.out.printf("Consistency errors:    %d%n", consistencyErrors.get());

        if (firstError.get() != null) {
            System.out.println();
            System.out.println("First error:");
            firstError.get().printStackTrace(System.out);
        }

        System.out.println();
        if (errors.get() == 0 && consistencyErrors.get() == 0 && allDone) {
            System.out.println("STRESS TEST PASSED");
        } else {
            System.out.println("STRESS TEST FAILED");
            System.exit(1);
        }
    }
}
