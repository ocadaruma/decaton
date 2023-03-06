/*
 * Copyright 2023 LINE Corporation
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

import static com.linecorp.decaton.processor.runtime.ProcessorProperties.CONFIG_PER_KEY_QUOTA_PROCESSING_RATE;
import static com.linecorp.decaton.processor.runtime.ProcessorProperties.CONFIG_PROCESSING_RATE;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import com.google.common.util.concurrent.RateLimiter;

import com.linecorp.decaton.client.DecatonClient;
import com.linecorp.decaton.processor.metrics.Metrics;
import com.linecorp.decaton.processor.runtime.PerKeyQuotaConfig;
import com.linecorp.decaton.processor.runtime.PerKeyQuotaConfig.QuotaCallback.Action;
import com.linecorp.decaton.processor.runtime.ProcessorProperties;
import com.linecorp.decaton.processor.runtime.ProcessorSubscription;
import com.linecorp.decaton.processor.runtime.ProcessorsBuilder;
import com.linecorp.decaton.processor.runtime.Property;
import com.linecorp.decaton.processor.runtime.StaticPropertySupplier;
import com.linecorp.decaton.processor.runtime.SubscriptionBuilder;
import com.linecorp.decaton.protobuf.ProtocolBuffersDeserializer;
import com.linecorp.decaton.protobuf.ProtocolBuffersSerializer;
import com.linecorp.decaton.protocol.Sample.HelloTask;
import com.linecorp.decaton.testing.KafkaClusterRule;
import com.linecorp.decaton.testing.TestUtils;
import com.linecorp.decaton.testing.processor.ProcessingGuarantee.GuaranteeType;
import com.linecorp.decaton.testing.processor.ProcessorTestSuite;

import io.micrometer.core.instrument.Counter;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.prometheus.client.exporter.HTTPServer;

public class RealPerKeyQuotaTest {
    @ClassRule
    public static KafkaClusterRule rule = new KafkaClusterRule();

    private String topic;
    private String shapingTopic;

    @Before
    public void setUp() {
        topic = rule.admin().createRandomTopic(3, 3);
        shapingTopic = topic + "-shaping";
        rule.admin().createTopic(shapingTopic, 3, 3);
    }

    @After
    public void tearDown() {
        rule.admin().deleteTopics(true, topic, shapingTopic);
    }

    @Test
    public void run() throws Exception {
        CountDownLatch terminationLatch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(terminationLatch::countDown));

        PrometheusMeterRegistry prometheusRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        Metrics.register(prometheusRegistry);
        new HTTPServer(new InetSocketAddress(8080), prometheusRegistry.getPrometheusRegistry(), true);

        Counter totalProcessedCount = Counter.builder("processed")
                                             .description("total processed tasks")
                                             .register(prometheusRegistry);

        Properties consumerConfig = new Properties();
        consumerConfig.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "my-decaton-processor");
        consumerConfig.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, rule.bootstrapServers());
        consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my-decaton-processor");

        ProcessorSubscription subscription =
                SubscriptionBuilder.newBuilder("pkq-processor")
                                   .properties(StaticPropertySupplier.of(
                                           Property.ofStatic(CONFIG_PER_KEY_QUOTA_PROCESSING_RATE, 500L)))
                                   .enablePerKeyQuota(PerKeyQuotaConfig.shape())
                                   .processorsBuilder(
                                           ProcessorsBuilder.consuming(topic,
                                                                       new ProtocolBuffersDeserializer<>(HelloTask.parser()))
                                                            .thenProcess((context, task) -> {
                                                                totalProcessedCount.increment();
                                                            })
                                   )
                                   .consumerConfig(consumerConfig)
                                   .buildAndStart();

        Properties producerProps = new Properties();
        producerProps.setProperty(ProducerConfig.CLIENT_ID_CONFIG, "my-decaton-client");
        producerProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, rule.bootstrapServers());
        producerProps.setProperty(ProducerConfig.LINGER_MS_CONFIG, "100");
        DecatonClient<HelloTask> client = DecatonClient
                .producing(topic, new ProtocolBuffersSerializer<HelloTask>())
                .applicationId("pkq-test")
                .instanceId("localhost")
                .producerConfig(producerProps)
                .build();

        int innocentKeys = 100;
        ExecutorService service = Executors.newFixedThreadPool(innocentKeys);
        for (int i = 0; i < innocentKeys; i++) {
            String key = "key" + i;
            RateLimiter limiter = RateLimiter.create(100L);
            service.execute(() -> {
                while (terminationLatch.getCount() > 0) {
                    limiter.acquire();
                    client.put(key, HelloTask.getDefaultInstance());
                }
            });
        }

        terminationLatch.await(180, TimeUnit.SECONDS);

        int burstingKeys = 5;
        ExecutorService burstingService = Executors.newFixedThreadPool(burstingKeys);
        for (int i = 0; i < burstingKeys; i++) {
            String key = "bursting-key" + i;
            RateLimiter limiter = RateLimiter.create(1000L);
            burstingService.execute(() -> {
                while (terminationLatch.getCount() > 0) {
                    limiter.acquire();
                    client.put(key, HelloTask.getDefaultInstance());
                }
            });

        }

        terminationLatch.await();
    }
}
