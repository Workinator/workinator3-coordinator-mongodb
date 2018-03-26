package com.allardworks.workinator3.coordinator.mongodb;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;

import java.time.Duration;
import java.time.Instant;
import java.util.Date;

import static java.lang.System.out;

@RequiredArgsConstructor
public class TimedActivity implements AutoCloseable {
    @NonNull
    private final String name;

    private final Date started = Date.from(Instant.now());
    private Date stopped;
    private boolean isRunning = true;

    public TimedActivity stop() {
        stopped = Date.from(Instant.now());
        isRunning = false;
        return this;
    }

    public Duration getElapsed() {
        return
                isRunning
                        ? Duration.ofMillis(Date.from(Instant.now()).getTime() - started.getTime())
                        : Duration.ofMillis(stopped.getTime() - started.getTime());
    }

    @Override
    public void close() {
        stop();
        val elapsed = stopped.getTime() - started.getTime();
        out.println("--------------------------" + name + ": " + elapsed + "ms");
    }
}
