package com.allardworks.workinator3.coordinator.mongodb.consumer;

import com.allardworks.workinator3.coordinator.mongodb.testsupport.DummyAsyncWorker;
import com.allardworks.workinator3.coordinator.mongodb.testsupport.DummyAsyncWorkerFactory;
import com.allardworks.workinator3.coordinator.mongodb.testsupport.DummyWorkinator;
import com.allardworks.workinator3.core.ConsumerConfiguration;
import com.allardworks.workinator3.core.ConsumerId;
import com.allardworks.workinator3.core.ConsumerRegistration;
import com.allardworks.workinator3.core.WorkerId;
import lombok.val;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;


public class ExecutorFactoryTests {
    /**
     * The type of executor factory necessary is based on the type of work factory
     * that is passed in. AsyncWorkerFactory results in the generation of an AsyncExecutorFactory.
     */
    @Test
    public void createAsyncExecutor() {
        val config = new ConsumerConfiguration();
        val workerFactory = new DummyAsyncWorkerFactory(DummyAsyncWorker::new);
        val factory = new ExecutorFactory(config, new DummyWorkinator());
        val id = new WorkerId(new ConsumerRegistration(new ConsumerId("boo"), ""), 1);
        val executor = factory.createExecutor(id, workerFactory);
        assertNotNull(executor);
    }
}
