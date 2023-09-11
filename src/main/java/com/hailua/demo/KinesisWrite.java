package com.hailua.demo;

import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.github.javafaker.Faker;
import com.hailua.demo.data.KinesisDatum;
import org.apache.commons.lang3.SerializationUtils;

import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.stream.IntStream;

public class KinesisWrite {
    public static final String KINESIS_STREAM = "test-stream";

    public static void main(String[] args) {
        // KPL
        Faker faker = new Faker();
        KinesisProducerConfiguration conf = new KinesisProducerConfiguration();
        conf.setRegion("us-east-1");
        KinesisProducer producer = new KinesisProducer(conf);
        List<KinesisDatum> data = IntStream.range(0, 10)
                .mapToObj(i -> new KinesisDatum(faker.harryPotter().character(), Timestamp.from(Instant.now())))
                .toList();
        data.forEach(d -> {
            ByteBuffer toSend = ByteBuffer.wrap(SerializationUtils.serialize(d));
            System.out.println("Shipping to Kinesis: " + d);
            producer.addUserRecord(KINESIS_STREAM, d.name().replace(" ", "_"), toSend); // TODO: understand why this doesn't seem to always send records
        });
        System.out.println("Flushing...");
        producer.flushSync();
        System.out.println("Flush complete.");
    }
}
