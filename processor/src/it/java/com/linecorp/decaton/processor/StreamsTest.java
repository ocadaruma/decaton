/*
 * Copyright 2020 LINE Corporation
 *
 * LINE Corporation licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.linecorp.decaton.processor;

import java.time.Duration;
import java.util.Properties;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import com.linecorp.decaton.testing.KafkaClusterRule;
import com.linecorp.decaton.testing.TestUtils;

public class StreamsTest {
    @ClassRule
    public static KafkaClusterRule rule = new KafkaClusterRule();

    String leftTopic;
    String rightTopic;

    @Before
    public void setUp() {
        leftTopic = rule.admin().createRandomTopic(1, 3);
        rightTopic = rule.admin().createRandomTopic(1, 3);
    }

    @After
    public void tearDown() {
        rule.admin().deleteTopics(true, leftTopic);
        rule.admin().deleteTopics(true, rightTopic);
    }

    @Test
    public void testJoin() throws Exception {
        StreamsBuilder builder = new StreamsBuilder();

        Serdes.StringSerde serde = new Serdes.StringSerde();
        KStream<String, String> streamX = builder.stream(leftTopic, Consumed.with(serde, serde));
        streamX.foreach((key, value) -> System.err.println("STREAM X: " + value));
        KStream<String, String> streamY = builder.stream(rightTopic, Consumed.with(serde, serde));
        streamY.foreach((key, value) -> System.err.println("STREAM Y: " + value));

        streamX.outerJoin(streamY,
                          (value1, value2) -> value1 + '/' + value2, JoinWindows.ofTimeDifferenceWithNoGrace(
                               Duration.ofSeconds(1)),
                          StreamJoined.with(serde, serde, serde))
               .foreach((key, value) -> System.err.printf("JOIN: %s = %s\n", key, value));

        Topology topology = builder.build();

        Properties props = new Properties();
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, rule.bootstrapServers());
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "test-streams");

        KafkaStreams streams = new KafkaStreams(topology, props);
        streams.start();

        Producer<String, String> producer = TestUtils.producer(rule.bootstrapServers(),
                                                               Serdes.String().serializer(),
                                                               Serdes.String().serializer());

        producer.send(new ProducerRecord<>(leftTopic, "A", "A"));
        Thread.sleep(100L);
        producer.send(new ProducerRecord<>(leftTopic, "B", "B"));
        Thread.sleep(100L);
        producer.send(new ProducerRecord<>(leftTopic, "C", "C"));
        Thread.sleep(100L);

        producer.send(new ProducerRecord<>(rightTopic, "B", "B"));
        Thread.sleep(100L);
        producer.send(new ProducerRecord<>(rightTopic, "D", "D"));

        Thread.sleep(10000000L);
    }
}
