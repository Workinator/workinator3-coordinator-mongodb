package com.allardworks.workinator3.coordinator.mongodb;

import lombok.Data;
import lombok.Getter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;


@Data
@Component
@ConfigurationProperties(prefix = "coordinator.monogdb")
public class MongoConfiguration {
    private String host = "localhost";
    private int port = 27017;
    private String databaseName = "Workinator";
    private String partitionsCollectionName = "partiions";
    private String consumersCollectionName = "consumers";
}
