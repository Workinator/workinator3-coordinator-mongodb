package com.allardworks.workinator3.coordinator.mongodb.core;

import com.allardworks.workinator3.core.NameUtility;
import lombok.val;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

public class NameUtilityTests {
    @Test
    public void getName() {
        val name = NameUtility.getRandomName();
        assertNotNull(name);
        System.out.println(name);
    }
}
