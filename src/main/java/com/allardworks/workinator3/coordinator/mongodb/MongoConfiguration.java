package com.allardworks.workinator3.coordinator.mongodb;

import lombok.Builder;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Getter
@Builder
public class MongoConfiguration {
    private final String host;
    private final int port;
    private final String databaseName;
    private final String partitionsCollectionName;
    private final String consumersCollectionName;

    public static class MongoConfigurationBuilder {
        private String host = "localhost";
        private int port = 27017;
        private String databaseName = "Workinator";
        private String partitionsCollectionName = "Partitions";
        private String consumersCollectionName = "Consumers";
    }
}
