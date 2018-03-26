package com.allardworks.workinator3.coordinator.mongodb.consumer;

import com.allardworks.workinator3.testsupport.TimedActivity;
import lombok.val;
import org.junit.Test;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static com.allardworks.workinator3.testsupport.TestUtility.waitFor;
import static org.junit.Assert.assertTrue;

/**
 * Created by jaya on 2/26/18.
 * k?
 */
public class ScheduledTaskThreadTests {
    /**
     * Make sure that the worker method fired at it's configured intervals.
     * @throws Exception
     */
    @Test
    public void firesAtInterval() {
        val hitCount = new AtomicInteger(0);
        val thread = new ScheduledTaskThread(Duration.ofMillis(100), hitCount::incrementAndGet);
        thread.start();
        try (val timer = new TimedActivity("2 hits")) {
            // first hit is immediate.
            // then delays 100 milliseconds and hits again.
            // 2 hits in about 100 milliseconds.
            waitFor(() -> hitCount.get() == 2);
            assertTrue(timer.getElapsed().toMillis() < 150);
        }
    }

    /**
     * Make sure the BLOCK aborts when the service is stopped.
     * @throws Exception
     */
    @Test
    public void delayInterruptedWhenStops() {
        val hitCount = new AtomicInteger(0);
        val thread = new ScheduledTaskThread(Duration.ofMillis(100), hitCount::incrementAndGet);
        thread.start();
        try (val timer = new TimedActivity("2 hits")) {
            // first his is immediate.
            // then delays 100 milliseconds and hits again.
            // 2 hits in about 100 milliseconds.
            waitFor(() -> hitCount.get() == 2);
            assertTrue(timer.getElapsed().toMillis() < 150);
        }

        // at this point, the run is blocking on the CountDownLatch.
        // the call to stop should release that block.
        // if the block is released, then the service will stop immediately.
        // if it's not working, then it wold take the 100ms timeout set on the block.
        try (val timer = new TimedActivity("stop")) {
            thread.stop();
            waitFor(() -> thread.getStatus().isStopped());
            assertTrue(timer.getElapsed().toMillis() < 40);
        }
    }
}
