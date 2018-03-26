package com.allardworks.workinator3.coordinator.mongodb.core;

import com.allardworks.workinator3.core.ConsumerConfiguration;
import lombok.val;
import org.junit.Test;

import java.time.Duration;

import static org.junit.Assert.assertEquals;

public class ConsumerConfigurationTests {
    @Test
    public void defaults() {
        val config = new ConsumerConfiguration();
        assertEquals(1, config.getMaxWorkerCount());
        assertEquals(Duration.ofSeconds(5), config.getDelayWhenNoAssignment());
        assertEquals(Duration.ofSeconds(30), config.getMinWorkTime());
    }
}
