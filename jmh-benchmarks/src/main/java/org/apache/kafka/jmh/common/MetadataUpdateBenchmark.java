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

package org.apache.kafka.jmh.common;

import org.apache.kafka.clients.MetadataSnapshot;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.MetadataResponse.PartitionMetadata;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Benchmarks the full metadata update workflow: MetadataSnapshot construction and Cluster view
 * computation. This exercises the cross-method pipeline from partition metadata through to the
 * indexed Cluster object.
 */
@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 3, time = 5)
@Measurement(iterations = 5, time = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class MetadataUpdateBenchmark {

    @Param({"100", "500", "5000"})
    private int topicCount;

    @Param({"10"})
    private int partitionsPerTopic;

    @Param({"3"})
    private int nodeCount;

    private Map<Integer, Node> nodes;
    private List<PartitionMetadata> partitions;
    private Set<String> internalTopics;
    private Map<String, Uuid> topicIds;

    @Setup
    public void setup() {
        nodes = new HashMap<>(nodeCount);
        for (int i = 0; i < nodeCount; i++) {
            nodes.put(i, new Node(i, "host" + i, 9092 + i));
        }
        nodes = Collections.unmodifiableMap(nodes);

        partitions = new ArrayList<>(topicCount * partitionsPerTopic);
        topicIds = new HashMap<>(topicCount);
        internalTopics = new HashSet<>();

        List<Integer> replicaIds = new ArrayList<>(nodeCount);
        for (int i = 0; i < nodeCount; i++) {
            replicaIds.add(i);
        }
        replicaIds = Collections.unmodifiableList(replicaIds);

        for (int t = 0; t < topicCount; t++) {
            String topicName = "topic-" + t;
            topicIds.put(topicName, Uuid.randomUuid());
            if (t % 50 == 0) {
                internalTopics.add(topicName);
            }

            for (int p = 0; p < partitionsPerTopic; p++) {
                int leaderId = (t * partitionsPerTopic + p) % nodeCount;
                partitions.add(new PartitionMetadata(
                        Errors.NONE,
                        new TopicPartition(topicName, p),
                        Optional.of(leaderId),
                        Optional.of(1),
                        replicaIds,
                        replicaIds,
                        Collections.emptyList()
                ));
            }
        }
    }

    /**
     * Benchmark the full MetadataSnapshot construction including Cluster view computation.
     * This is the hot path executed on every metadata response.
     */
    @Benchmark
    public MetadataSnapshot metadataSnapshotConstruction() {
        return new MetadataSnapshot(
                "cluster-1",
                nodes,
                partitions,
                Collections.emptySet(),
                Collections.emptySet(),
                internalTopics,
                nodes.get(0),
                topicIds
        );
    }

    /**
     * Benchmark the Cluster construction directly (the view built by MetadataSnapshot).
     * This isolates the Cluster indexing cost.
     */
    @Benchmark
    public Cluster clusterConstruction() {
        // Simulate what computeClusterView does: convert PartitionMetadata -> PartitionInfo,
        // then build Cluster
        List<org.apache.kafka.common.PartitionInfo> partitionInfos = new ArrayList<>(partitions.size());
        for (PartitionMetadata pm : partitions) {
            partitionInfos.add(MetadataResponse.toPartitionInfo(pm, nodes));
        }
        return new Cluster("cluster-1", nodes.values(), partitionInfos,
                Collections.emptySet(), Collections.emptySet(), internalTopics,
                nodes.get(0), topicIds);
    }

    public static void main(String[] args) throws RunnerException {
        new Runner(new OptionsBuilder()
                .include(MetadataUpdateBenchmark.class.getSimpleName())
                .build()).run();
    }
}
