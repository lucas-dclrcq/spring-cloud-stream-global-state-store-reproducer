package com.example.gstatestorereproducer;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.junit.jupiter.api.*;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class GstateStoreReproducerUnitTests {
    TopologyTestDriver testDriver;

    String incomingTopic = "incoming";
    String outgoingTopic = "outgoing";
    String productInfoTopic = "product-info";

    Properties getStreamsConfiguration() {
        var streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:123");
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        return streamsConfiguration;
    }

    @BeforeEach
    void setup() {
        var builder = new StreamsBuilder();

        var incomingInput = builder.stream(
                incomingTopic,
                Consumed.with(Serdes.String(), Serdes.String())
        );

        GstateStoreReproducerApplication.configureGlobalStateStore(builder);


        new GstateStoreReproducerApplication()
                .process()
                .apply(incomingInput)
                .to(outgoingTopic, Produced.with(Serdes.String(), Serdes.String()));

        testDriver = new TopologyTestDriver(builder.build(), getStreamsConfiguration());
    }

    @AfterEach
    void cleanUp() {
        testDriver.close();
    }

    @Test
    void given_io_event_should_map_product_info() {
        // ARRANGE

        final TestInputTopic<String, String> productTopic = testDriver.createInputTopic(productInfoTopic, new StringSerializer(), new StringSerializer());
        final TestInputTopic<String, String> inputTopic = testDriver.createInputTopic(incomingTopic, new StringSerializer(), new StringSerializer());
        final TestOutputTopic<String, String> outputTopic = testDriver.createOutputTopic(outgoingTopic, new StringDeserializer(), new StringDeserializer());

        // ACT
        productTopic.pipeInput("PRODUCT1", "A");
        inputTopic.pipeInput("PRODUCT1", "B");

        // ASSERT
        assertThat(outputTopic.readValue()).isEqualTo("A");
    }
}