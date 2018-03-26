package com.allardworks.workinator3.coordinator.mongodb.testsupport;

import com.allardworks.workinator3.contracts.Service;
import lombok.val;
import org.junit.Assert;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static java.lang.System.out;
import static org.assertj.core.api.Fail.fail;

public class TestUtility {
    private TestUtility() {
    }

    public static void startAndWait(final Service service) {
        if (service.getStatus().isStarted()) {
            return;
        }

        val countdown = new CountDownLatch(1);
        service.getTransitionEventHandlers().onPostStarted(t -> countdown.countDown());
        service.start();
        try {
            if (!countdown.await(1, TimeUnit.SECONDS)) {
                fail("Didn't start in time");
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void stopAndWait(final Service service) {
        if (service.getStatus().isStopped()) {
            return;
        }

        val countdown = new CountDownLatch(1);
        service.getTransitionEventHandlers().onPostStopped(t -> countdown.countDown());
        service.stop();
        try {
            if (!countdown.await(1, TimeUnit.SECONDS)) {
                fail("Didn't stop in time");
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (Exception ex) {
            out.println();
        }
    }

    public static void waitFor(final Supplier<Boolean> done) {
        val start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < 1000) {
            if (done.get()) {
                return;
            }

            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        Assert.fail("waitFor didn't finish in time.");
    }
}
