package com.allardworks.workinator3.coordinator.mongodb;

import com.allardworks.workinator3.core.PartitionConfiguration;
import com.mongodb.*;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import lombok.Getter;
import lombok.NonNull;
import lombok.val;
import org.bson.Document;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.springframework.stereotype.Service;

import static com.allardworks.workinator3.coordinator.mongodb.DocumentUtility.doc;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

@Service
public class MongoDal implements AutoCloseable {
    @Getter
    private final MongoConfiguration config;

    @Getter
    private final MongoClient client;

    @Getter
    private final MongoCollection<Document> partitionsCollection;

    @Getter
    private final MongoCollection<Document> consumersCollection;

    @Getter
    private final MongoDatabase database;

    public MongoDal(@NonNull MongoConfiguration config) {
        this.config = config;


        val address = new ServerAddress(config.getHost(), config.getPort());

        val pojoCodecRegistry = fromRegistries(MongoClient.getDefaultCodecRegistry(),
                fromProviders(PojoCodecProvider.builder().automatic(true).build()));

        val options =
                MongoClientOptions
                .builder()
                .codecRegistry(pojoCodecRegistry)
                        .writeConcern(WriteConcern.MAJORITY)
                .build();


        client = new MongoClient(address, options);
        database = client.getDatabase(config.getDatabaseName());
        partitionsCollection = database.getCollection(config.getPartitionsCollectionName(), Document.class);
        consumersCollection = database.getCollection(config.getConsumersCollectionName(), Document.class);
        setupDatabase();
    }

    @Override
    public void close() {
        client.close();
    }

    /**
     * Gets the partition configuration from the database.
     * @param partitionKey
     * @return
     */
    public PartitionConfiguration getPartitionConfiguration(final String partitionKey) {
        val where = doc("partitionKey", partitionKey);
        val found = getPartitionsCollection().find(where).first();
        if (found == null) {
            return null;
        }

        return
                PartitionConfiguration
                        .builder()
                        .partitionKey(partitionKey)
                        .maxIdleTimeSeconds(((Document)found.get("configuration")).getInteger("maxIdleTimeSeconds"))
                        .maxWorkerCount(((Document)found.get("configuration")).getInteger("maxWorkerCount"))
                        .build();
    }


    private void setupDatabase() {
        // ------------------------------------------------
        // consumers - primary key
        // ------------------------------------------------
        {
            val pkConsumer = new BasicDBObject().append("name", 1);
            val pkConsumerOptions = new IndexOptions().name("name").unique(true).background(true);
            consumersCollection.createIndex(pkConsumer, pkConsumerOptions);
        }

        // ------------------------------------------------
        // partitions - primary key
        // ------------------------------------------------
        {
            val pkPartition = new BasicDBObject().append("partitionKey", 1);
            val pkPartitionOptions = new IndexOptions().name("partitionKey").unique(true).background(true);
            partitionsCollection.createIndex(pkPartition, pkPartitionOptions);
        }


        // ------------------------------------------------
        //  most rules sort by last checked date.
        // ------------------------------------------------
        {
            val date = doc("lastCheckedDate", 1);
            val dateOptions = new IndexOptions().name("lastCheckedDate").background(true);
            partitionsCollection.createIndex(date, dateOptions);
        }

        /*

        seems like an appropriate index, but doesn't affect performance with 25,000 partitions

         */

        // ------------------------------------------------
        // rule 1 is based on worker count and due date
        // ------------------------------------------------
        {
            val rule1 = doc("workerCount", 1, "dueDate", 1);
            val rule1Options = new IndexOptions().name("rule1").background(false);
            partitionsCollection.createIndex(rule1, rule1Options);
        }
    }
}
