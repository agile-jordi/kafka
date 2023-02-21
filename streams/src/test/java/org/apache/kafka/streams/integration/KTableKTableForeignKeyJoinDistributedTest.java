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

package org.apache.kafka.streams.integration;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.ThreadMetadata;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.DefaultProductionExceptionHandler;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.quietlyCleanStateAfterTest;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.safeUniqueTestName;

@Timeout(600)
@Category({IntegrationTest.class})
public class KTableKTableForeignKeyJoinDistributedTest {

    private List<KeyValue<String, String>> getCombinationsOf2LettersAndOneRhsValue() {
        final List<KeyValue<String, String>> combinations = new ArrayList<>();
        for (char c1 = 'a'; c1 <= 'z'; c1++) {
            for (char c2 = 'a'; c2 <= 'z'; c2++) {
                for (char c3 = 'a'; c3 <= 'a'; c3++) {
                    for (int d = 1; d <= 5; d++) {
                        final String letters = "" + c1 + c2 + c3;
                        combinations.add(new KeyValue<>(letters + d, letters + "|rhs" + d));
                    }
                }
            }
        }
        return combinations;
    }

    private static final int NUM_BROKERS = 1;
    private static final String LEFT_TABLE = "left_table";
    private static final String RIGHT_TABLE = "right_table";
    private static final String OUTPUT = "output-topic";
//    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);

//    @BeforeAll
//    public static void startCluster() throws IOException, InterruptedException {
//        CLUSTER.start();
//    }
//
//    @AfterAll
//    public static void closeCluster() {
//        CLUSTER.stop();
//    }

    private static final Properties CONSUMER_CONFIG = new Properties();

    private static final String INPUT_TOPIC = "input-topic";

    private KafkaStreams client1;
//    private KafkaStreams client2;

    private volatile boolean client1IsOk = false;
    private volatile boolean client2IsOk = false;

    final List<KeyValue<String, String>> leftTable = getCombinationsOf2LettersAndOneRhsValue();

    final List<KeyValue<String, String>> rightTable = Arrays.asList(
            new KeyValue<>("rhs1", "rhsValue1"),
            new KeyValue<>("rhs2", "rhsValue2"),
            new KeyValue<>("rhs3", "rhsValue3"),
            new KeyValue<>("rhs4", "rhsValue4"),
            new KeyValue<>("rhs5", "rhsValue5")
    );

    final Set<KeyValue<String, String>> expectedResult = leftTable.stream()
            .map(s -> {
                final String number = s.value.substring(s.value.length() - 1);
                return new KeyValue<>(s.value.split("\\|")[0] + number, "(" + s.value + "," + "rhsValue" + number + ")");
            })
            .collect(Collectors.toSet());


    private KafkaContainer kafka;


    @BeforeEach
    public void setupTopics() throws InterruptedException {
        kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.3.1"));
        kafka.start();
        final AdminClient adminClient = KafkaAdminClient.create(Collections.singletonMap(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()));
        adminClient.createTopics(Collections.singletonList(new NewTopic(LEFT_TABLE, 5, (short) 1)));
        adminClient.createTopics(Collections.singletonList(new NewTopic(RIGHT_TABLE, 5, (short) 1)));
        adminClient.createTopics(Collections.singletonList(new NewTopic(OUTPUT, 5, (short) 1)));

        //Fill test tables
        final Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        final Time time = new SystemTime();
        IntegrationTestUtils.produceKeyValuesSynchronously(LEFT_TABLE, leftTable, producerConfig, time);
        IntegrationTestUtils.produceKeyValuesSynchronously(RIGHT_TABLE, rightTable, producerConfig, time);

        CONSUMER_CONFIG.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        CONSUMER_CONFIG.put(ConsumerConfig.GROUP_ID_CONFIG, "ktable-ktable-distributed-consumer");
        CONSUMER_CONFIG.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        CONSUMER_CONFIG.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    }

    @AfterEach
    public void after() {
        client1.close();
//        client2.close();
        quietlyCleanStateAfterTest(null, client1);
//        quietlyCleanStateAfterTest(CLUSTER, client2);
    }

    public Properties getStreamsConfiguration(final TestInfo testInfo) {
        final String safeTestName = safeUniqueTestName(getClass(), testInfo);

        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "app-" + safeTestName);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        streamsConfiguration.put(ProducerConfig.ACKS_CONFIG, "all");
        streamsConfiguration.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        streamsConfiguration.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 50);
        streamsConfiguration.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 700);
        streamsConfiguration.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 1048576);
//        streamsConfiguration.put(
//                StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
//                LogAndContinueExceptionHandler.class.getCanonicalName()
//        );
        streamsConfiguration.put(
                StreamsConfig.DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG,
                DefaultProductionExceptionHandler.class.getCanonicalName()
        );
        streamsConfiguration.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, "10");
        streamsConfiguration.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, "true");
        streamsConfiguration.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, "1");
//        streamsConfiguration.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.AT_LEAST_ONCE);
        streamsConfiguration.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 0);
        streamsConfiguration.put(StreamsConfig.MAX_WARMUP_REPLICAS_CONFIG, 1);
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1_000);
        streamsConfiguration.put(StreamsConfig.restoreConsumerPrefix(ConsumerConfig.MAX_POLL_RECORDS_CONFIG), 20000);
//        streamsConfiguration.put(StreamsConfig.STATE_CLEANUP_DELAY_MS_CONFIG, 3_600_000);
//        streamsConfiguration.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30_000);
        streamsConfiguration.put(StreamsConfig.producerPrefix(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG), 1_000);
        streamsConfiguration.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 999);
        return streamsConfiguration;
    }

    private void configureBuilder(final StreamsBuilder builder) {
        final KTable<String, String> left = builder.table(
                LEFT_TABLE,
                Consumed.with(Serdes.String(), Serdes.String())
                        .withOffsetResetPolicy(Topology.AutoOffsetReset.EARLIEST)
                        .withName("readLeft"),
                Materialized.as("left-store")
        );
        final KTable<String, String> right = builder.table(
                RIGHT_TABLE,
                Consumed.with(Serdes.String(), Serdes.String())
                        .withOffsetResetPolicy(Topology.AutoOffsetReset.EARLIEST)
                        .withName("readRight"),
                Materialized.as("right-store")
        );

        final Function<String, String> extractor = value -> value.split("\\|")[1];
        final ValueJoiner<String, String, String> joiner = (value1, value2) -> "(" + value1 + "," + value2 + ")";

        final KTable<String, String> fkJoin = left.join(right, extractor, joiner);
        fkJoin
                .toStream()
                .to(OUTPUT, Produced.with(Serdes.String(), Serdes.String()));
    }

    @RepeatedTest(10)
    public void shouldBeInitializedWithDefaultSerde(final TestInfo testInfo) throws Exception {
        final Properties streamsConfiguration1 = getStreamsConfiguration(testInfo);
        final Properties streamsConfiguration2 = getStreamsConfiguration(testInfo);

        final String appId = streamsConfiguration1.getProperty(StreamsConfig.APPLICATION_ID_CONFIG);

        //Each streams client needs to have it's own StreamsBuilder in order to simulate
        //a truly distributed run
        final StreamsBuilder builder1 = new StreamsBuilder();
        configureBuilder(builder1);
        final StreamsBuilder builder2 = new StreamsBuilder();
        configureBuilder(builder2);


        createClients(
                builder1.build(streamsConfiguration1),
                streamsConfiguration1,
                builder2.build(streamsConfiguration2),
                streamsConfiguration2
        );

        setStateListenersForVerification(thread -> !thread.activeTasks().isEmpty());

        startClients();

//        waitUntilBothClientAreOK(
//              "At least one client did not reach state RUNNING with active tasks"
//        );

        IntegrationTestUtils
                .waitUntilFinalKeyValueRecordsReceived(CONSUMER_CONFIG, appId, OUTPUT, new ArrayList<>(expectedResult), 3 * 60 * 1000);

        //Check that both clients are still running
//        waitUntilBothClientAreOK("At least one client did not reach state RUNNING with active tasks");
    }

    private void createClients(final Topology topology1,
                               final Properties streamsConfiguration1,
                               final Topology topology2,
                               final Properties streamsConfiguration2) {

        client1 = new KafkaStreams(topology1, streamsConfiguration1);
//        client2 = new KafkaStreams(topology2, streamsConfiguration2);
    }

    private void setStateListenersForVerification(final Predicate<ThreadMetadata> taskCondition) {
        client1.setStateListener((newState, oldState) -> {
            if (newState == KafkaStreams.State.RUNNING &&
                    client1.metadataForLocalThreads().stream().allMatch(taskCondition)) {
                client1IsOk = true;
            }
        });
//        client2.setStateListener((newState, oldState) -> {
//            if (newState == KafkaStreams.State.RUNNING &&
//                  client2.metadataForLocalThreads().stream().allMatch(taskCondition)) {
//                client2IsOk = true;
//            }
//        });
    }

    private void startClients() {
        client1.start();
//        client2.start();
    }

    private void waitUntilBothClientAreOK(final String message) throws Exception {
        TestUtils.waitForCondition(() -> client1IsOk && client2IsOk,
                60 * 1000,
                message + ": "
                        + "Client 1 is " + (!client1IsOk ? "NOT " : "") + "OK, "
                        + "client 2 is " + (!client2IsOk ? "NOT " : "") + "OK."
        );
    }
}
