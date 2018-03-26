package com.allardworks.workinator3.coordinator.mongodb.mongo2;

import com.allardworks.workinator3.coordinator.mongodb.WorkinatorTester;
import com.allardworks.workinator3.coordinator.mongodb.WorkinatorTests;

public class MongoWorkinatorTests extends WorkinatorTests {
    @Override
    protected WorkinatorTester getTester() {
        return new MongoWorkinatorTester();
    }
}
