package com.allardworks.workinator3.coordinator.mongodb.consumer;

import com.allardworks.workinator3.consumer.config.ConsumerConfiguration;
import com.allardworks.workinator3.contracts.*;
import com.allardworks.workinator3.testsupport.DummyAsyncWorker;
import com.allardworks.workinator3.testsupport.DummyAsyncWorkerFactory;
import com.allardworks.workinator3.testsupport.DummyWorkinator;
import com.allardworks.workinator3.testsupport.TestUtility;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

import java.util.Date;

import static com.allardworks.workinator3.testsupport.TestUtility.startAndWait;

public class ExecutorAsyncTests {

    /**
     * Starts an executor.
     * Makes sure it does work.
     * Then stops it.
     *
     * @throws InterruptedException
     */
    @Test
    public void startAndStop() throws Exception {
        val configuration = new ConsumerConfiguration();
        val consumerId = new ConsumerId("booyea");
        val registration = new ConsumerRegistration(consumerId, "");
        val workerId = new WorkerId(registration, 1);
        val worker = new DummyAsyncWorker();
        val factory = new DummyAsyncWorkerFactory(() -> worker);
        val workinator = new DummyWorkinator();

        workinator.setNextAssignment(new Assignment(workerId, "ab", "", "", new Date()));
        val id = new WorkerId(new ConsumerRegistration(new ConsumerId("boo"), ""), 1);
        try (val executor = new ExecutorAsync(id, configuration, factory, workinator)) {
            startAndWait(executor);
            TestUtility.waitFor(() -> worker.getLastContext() != null);
            TestUtility.stopAndWait(executor);
            Assert.assertTrue(worker.getHitCount() > 0);
        }
    }

    /**
     * Put the worker into an infinite loop (freeze), then
     * stop the executor. See that the STOP won't complete
     * until the worker is thawed.
     *
     * @throws Exception
     */
    @Test
    public void wontStopWhileWorkerFIsBusy() throws Exception {
        val freezeTime = 100;
        val configuration = new ConsumerConfiguration();
        val consumerId = new ConsumerId("booyea");
        val registration = new ConsumerRegistration(consumerId, "");
        val workerId = new WorkerId(registration, 1);
        val workinator = new DummyWorkinator();
        val worker = new DummyAsyncWorker();
        val workerFactory = new DummyAsyncWorkerFactory(() -> worker);

        workinator.setNextAssignment(new Assignment(workerId, "ab", "", "", new Date()));
        val id = new WorkerId(new ConsumerRegistration(new ConsumerId("aaa"), ""),1);
        try (val executor = new ExecutorAsync(id, configuration, workerFactory, workinator)) {
            startAndWait(executor);
            TestUtility.waitFor(() -> worker.getLastContext() != null);

            // freeze the thread
            worker.setFrozen(true);

            // thaw it after some time
            new Thread(() -> {
                try {
                    Thread.sleep(freezeTime);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                worker.setFrozen(false);
            }).start();

            val start = System.currentTimeMillis();
            TestUtility.stopAndWait(executor);
            val stop = System.currentTimeMillis();

            // make sure the stop took as long as it should have
            Assert.assertTrue(stop - start >= freezeTime);
            Assert.assertTrue(worker.getHitCount() > 0);
        }
    }
}

