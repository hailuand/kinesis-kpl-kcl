package com.hailua.demo.consumer;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.model.Record;
import com.hailua.demo.data.KinesisDatum;
import org.apache.commons.lang3.SerializationUtils;

import java.util.List;

// TODO: Try v2
public class KinesisRecordProcessor implements IRecordProcessor {
    @Override
    public void initialize(String shardId) {
        System.out.println("Initialized for shard ID: " + shardId + ".");
    }

    @Override
    public void processRecords(List<Record> records, IRecordProcessorCheckpointer iRecordProcessorCheckpointer) {
        records.forEach(r -> {
            KinesisDatum datum = SerializationUtils.deserialize(r.getData().array());
            System.out.println("Processing " + datum.toString() + ".");
        });
        checkpoint(iRecordProcessorCheckpointer);
    }

    @Override
    public void shutdown(IRecordProcessorCheckpointer iRecordProcessorCheckpointer, ShutdownReason shutdownReason) {
        if(shutdownReason.equals(ShutdownReason.TERMINATE)) {
            System.out.println("Received termination request.");
            checkpoint(iRecordProcessorCheckpointer);
        }
    }

    private void checkpoint(IRecordProcessorCheckpointer checkpointer) {
        try {
            System.out.println("Checkpointing.");
            checkpointer.checkpoint();
        } catch (InvalidStateException e) {
            System.out.println("Not in the correct state to checkpoint.");
            throw new RuntimeException(e);
        } catch (ShutdownException e) {
            System.out.println("Oh dear - this has gone pear-shaped.");
            throw new RuntimeException(e);
        }
    }

}
