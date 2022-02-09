package com.example.gstatestorereproducer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@EmbeddedKafka(
        topics = {"incoming", "outgoing", "product-info"},
        brokerProperties = "log.dir=target/${random.uuid}/embedded-kafka",
        bootstrapServersProperty = "spring.cloud.stream.kafka.binder.brokers"
)
@DirtiesContext
@TestPropertySource(properties = {
        "spring.kafka.bootstrap-servers=${spring.cloud.stream.kafka.binder.brokers}"
})
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class GstateStoreReproducerApplicationIntegrationTest {
    @Autowired
    EmbeddedKafkaBroker embeddedKafkaBroker;

    DefaultKafkaProducerFactory<String, String> producerFactory;
    DefaultKafkaConsumerFactory<String, String> consumerFactory;


    @BeforeAll
    void setup() {
        final Map<String, Object> producerProps = KafkaTestUtils.producerProps(embeddedKafkaBroker);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        producerFactory = new DefaultKafkaProducerFactory<>(producerProps);

        final Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(embeddedKafkaBroker.getBrokersAsString(), "data-integraton", "true");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        consumerFactory = new DefaultKafkaConsumerFactory<>(consumerProps);
    }

    @Test
    void test() {
        // ARRANGE
        final KafkaTemplate<String, String> incomingProducer = new KafkaTemplate<>(producerFactory, true);
        incomingProducer.setDefaultTopic("incoming");
        final KafkaTemplate<String, String> productInfoProducer = new KafkaTemplate<>(producerFactory, true);
        productInfoProducer.setDefaultTopic("product-info");
        final Consumer<String, String> consumer = consumerFactory.createConsumer("consumer-group", "consumer");
        embeddedKafkaBroker.consumeFromAnEmbeddedTopic(consumer, "outgoing");

        // ACT
        productInfoProducer.sendDefault("PRODUCT1", "A");
        incomingProducer.sendDefault("PRODUCT1", "B");

        // ASSERT
        final ConsumerRecords<String, String> records = KafkaTestUtils.getRecords(consumer);
        assertThat(records)
                .flatExtracting(
                        ConsumerRecord::key,
                        ConsumerRecord::value
                )
                .containsExactly(
                        "PRODUCT1",
                        "A"
                );

    }
}