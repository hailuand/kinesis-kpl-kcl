package com.hailua.demo;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.hailua.demo.consumer.KinesisRecordProcessorFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;

import static com.hailua.demo.KinesisWrite.KINESIS_STREAM;

public class KinesisRead {
    public static void main(String[] args) throws UnknownHostException {
        // KCL
        String workerId = InetAddress.getLocalHost().getCanonicalHostName() + ":" + UUID.randomUUID();
        KinesisClientLibConfiguration kinesisClientLibConfiguration = new KinesisClientLibConfiguration("test-app",
                KINESIS_STREAM, new ProfileCredentialsProvider(), workerId);
        Worker kclWorker = new Worker.Builder()
                .config(kinesisClientLibConfiguration)
                .recordProcessorFactory(new KinesisRecordProcessorFactory())
                .build();
        System.out.println("Spinning up KCL worker.");
        kclWorker.run();
    }
}
