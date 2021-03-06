package com.allardworks.workinator3.coordinator.mongodb;

import com.allardworks.workinator3.core.*;
import com.allardworks.workinator3.core.commands.CreatePartitionCommand;
import com.allardworks.workinator3.core.commands.ReleaseAssignmentCommand;
import lombok.RequiredArgsConstructor;
import lombok.val;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Created by jaya on 3/4/18.
 * k?
 */
@RequiredArgsConstructor
public class WorkinatorTestHarness implements AutoCloseable {
    private final WorkinatorTester tester;
    private final Map<String, WorkerStatus> workers = new HashMap<>();
    private final Map<String, Assignment> assignments = new HashMap<>();

    private WorkerStatus createWorkerStatus(final String consumerId) {
        return new WorkerStatus(new WorkerId(new ConsumerRegistration(new ConsumerId(consumerId), ""), 1));
    }

    public WorkinatorTester getTester() {
        return tester;
    }

    @Override
    public void close() throws Exception {
        tester.close();
    }

    public WorkinatorTestHarness createPartition(final String partitionKey) throws Exception {
        return createPartition(partitionKey, 1);
    }

    public WorkinatorTestHarness createPartition(final String partitionKey, int maxWorkers) throws Exception {
        tester
                .getWorkinator()
                .createPartition(
                        CreatePartitionCommand
                                .builder()
                                .partitionKey(partitionKey)
                                .maxWorkerCount(maxWorkers)
                                .build());
        return this;
    }

    public WorkinatorTestHarness setPartitionHasWork(final String partitionKey) {
        tester.setHasWork(partitionKey, true);
        return this;
    }

    public WorkinatorTestHarness createWorker(final String workerName) {
        val worker = createWorkerStatus(workerName);
        workers.put(workerName, worker);
        return this;
    }

    public WorkinatorTestHarness createWorkers(final String... workerNames) {
        for (val w : workerNames) {
            createWorker(w);
        }
        return this;
    }

    private Assignment getAssignment(final String workerName) {
        val worker = workers.get(workerName);
        val assignment = tester.getWorkinator().getAssignment(worker);
        worker.setCurrentAssignment(assignment);
        return assignment;
    }

    /**
     * Gets a new assignment.
     *
     * @param workerName
     * @param expectedPartitionKey
     * @param expectedRule
     * @return
     */
    public WorkinatorTestHarness assertGetAssignment(final String workerName, final String expectedPartitionKey, final String expectedRule) {
        val existing = assignments.get(workerName);

        // get the new assignment
        val assignment = getAssignment(workerName);

        // release the old one if it's different
        // todo: a little hacky. WorkerRunnerProvider is doing the same thing.
        if (existing != null && !existing.getPartitionKey().equals(assignment.getPartitionKey())) {
            tester.getWorkinator().releaseAssignment(new ReleaseAssignmentCommand(existing));
        }

        assertEquals(expectedPartitionKey, assignment.getPartitionKey());
        assertEquals(expectedRule, assignment.getRuleName());
        assignments.put(workerName, assignment);
        return this;
    }

    public WorkinatorTestHarness assertGetAssignmentNull(final String workerName) {
        assertNull(getAssignment(workerName));
        return this;
    }

    public WorkinatorTestHarness saveWorkersStatus() {
        throw new NotImplementedException();
        //tester.getWorkinator().updateWorkerStatus(new UpdateWorkersStatusCommand(new ArrayList<>(workers.values())));
        //return this;
    }

    public WorkinatorTestHarness assertNullAssignment(final String workerName) {
        val assignment = getAssignment(workerName);
        assertNull(assignment);
        return this;
    }

    public WorkinatorTestHarness releaseAssignment(final String workerName) {
        val assignment = assignments.get(workerName);
        tester.getWorkinator().releaseAssignment(new ReleaseAssignmentCommand(assignment));
        assignments.remove(workerName);
        return this;
    }

    public WorkinatorTestHarness setWorkerHasWork(final String workerName) {
        throw new NotImplementedException();
        //val worker = workers.get(workerName);
        //worker.setHasWork(true);
        //return this;
    }

    public WorkinatorTestHarness setWorkersHaveWork(final String... workers) {
        for (val w : workers) {
            setWorkerHasWork(w);
        }
        return this;
    }

    public WorkinatorTestHarness setWorkerDoesntHaveWork(final String workerName) {
        throw new NotImplementedException();
        //val worker = workers.get(workerName);
        //worker.setHasWork(false);
        //return this;
    }
}
