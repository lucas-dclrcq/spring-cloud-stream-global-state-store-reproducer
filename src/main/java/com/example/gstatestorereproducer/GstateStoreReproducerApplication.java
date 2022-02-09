package com.example.gstatestorereproducer;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.internals.KeyValueStoreBuilder;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.kafka.StreamsBuilderFactoryBeanCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.StreamsBuilderFactoryBeanConfigurer;

import java.util.function.Consumer;
import java.util.function.Function;

@SpringBootApplication
public class GstateStoreReproducerApplication {
    public final static String PRODUCT_INFO_STORE = "product-info-store";
    public final static String PRODUCT_INFO_TOPIC = "product-info";

    public static void main(String[] args) {
        SpringApplication.run(GstateStoreReproducerApplication.class, args);
    }

    public static void configureGlobalStateStore(StreamsBuilder streamsBuilder) {
        final var keyValueStore = new KeyValueStoreBuilder<>(
                Stores.inMemoryKeyValueStore(PRODUCT_INFO_STORE),
                Serdes.String(),
                Serdes.String(),
                Time.SYSTEM
        );

        streamsBuilder.addGlobalStore(
                keyValueStore.withLoggingDisabled(),
                PRODUCT_INFO_TOPIC,
                Consumed.with(Serdes.String(), Serdes.String()),
                () -> new Processor<>() {
                    KeyValueStore<String, String> store;

                    @Override
                    public void init(ProcessorContext<Void, Void> context) {
                        store = context.getStateStore(PRODUCT_INFO_STORE);
                    }

                    @Override
                    public void process(Record<String, String> record) {
                        store.put(record.key(), record.value());
                    }
                }
        );
    }

    @Bean
    public StreamsBuilderFactoryBeanConfigurer streamsBuilderFactoryBeanCustomizer() {
        return factoryBean -> {
                try {
                    final StreamsBuilder streamsBuilder = factoryBean.getObject();
                    configureGlobalStateStore(streamsBuilder);
                } catch (Exception e) {
                    e.printStackTrace();
                }
        };
    }

    @Bean
    public Function<KStream<String, String>, KStream<String, String>> process() {
        return input -> input.transform(() -> new Transformer<String, String, KeyValue<String, String>>() {
            KeyValueStore<String, String> store;

            @Override
            public void init(org.apache.kafka.streams.processor.ProcessorContext context) {
                store = context.getStateStore(PRODUCT_INFO_STORE);
            }

            @Override
            public KeyValue<String, String> transform(String key, String value) {
                return new KeyValue<>(key, store.get(key));
            }

            @Override
            public void close() {}
        });
    }
}
