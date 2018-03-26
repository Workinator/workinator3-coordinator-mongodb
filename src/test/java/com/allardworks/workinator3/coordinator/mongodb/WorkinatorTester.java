package com.allardworks.workinator3.coordinator.mongodb;

import com.allardworks.workinator3.core.Workinator;

public interface WorkinatorTester extends AutoCloseable {
    Workinator getWorkinator();
    void setHasWork(String partitionKey, boolean hasMoreWork);
    void setDueDateFuture(String partitionKey);
}
