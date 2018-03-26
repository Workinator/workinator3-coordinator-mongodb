package com.allardworks.workinator3.coordinator.mongodb.testsupport;

import com.allardworks.workinator3.core.*;
import com.allardworks.workinator3.core.commands.*;

import java.util.List;

public class DummyWorkinator implements Workinator {
    private Assignment next;

    public void setNextAssignment(final Assignment assignment) {
        next = assignment;
    }

    @Override
    public Assignment getAssignment(WorkerStatus executorId) {
        return next;
    }

    @Override
    public void releaseAssignment(ReleaseAssignmentCommand assignment) {

    }

    @Override
    public ConsumerRegistration registerConsumer(RegisterConsumerCommand command) {
        return null;
    }

    @Override
    public void unregisterConsumer(UnregisterConsumerCommand command) {

    }

    @Override
    public void createPartition(CreatePartitionCommand command) {

    }

    @Override
    public void updateWorkerStatus(UpdateWorkersStatusCommand workerStatus) {

    }

    @Override
    public void updateConsumerStatus(UpdateConsumerStatusCommand consumerStatus) {

    }

    @Override
    public List<PartitionInfo> getPartitions() {
        return null;
    }

    @Override
    public PartitionConfiguration getPartitionConfiguration(String partitionKey) {
        return null;
    }

    @Override
    public void close() {

    }
}
