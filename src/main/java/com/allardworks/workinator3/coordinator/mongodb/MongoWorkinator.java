package com.allardworks.workinator3.coordinator.mongodb;

import com.allardworks.workinator3.coordinator.AssignmentStrategy;
import com.allardworks.workinator3.core.*;
import com.allardworks.workinator3.core.commands.*;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoWriteException;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.bson.Document;
import org.springframework.stereotype.Service;
import com.allardworks.workinator3.core.Assignment;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static com.allardworks.workinator3.coordinator.mongodb.DocumentUtility.doc;
import static com.allardworks.workinator3.core.ConvertUtility.MIN_DATE;

/**
 * Mongo implementation of the workinator.
 */
@Slf4j
@RequiredArgsConstructor
@Service
public class MongoWorkinator implements Workinator {
    private final MongoDal dal;
    private final PartitionConfigurationCache configurationCache;
    private final AssignmentStrategy assignmentStrategy;

    /**
     * Create a partition.
     *
     * @param command
     * @throws PartitionExistsException
     */
    public void createPartition(final CreatePartitionCommand command) throws PartitionExistsException {
        val create = doc(
                // key
                "partitionKey", command.getPartitionKey(),

                // info
                "createDate", new Date(),

                // configuration
                "configuration", doc(
                        "maxIdleTimeSeconds", command.getMaxIdleTimeSeconds(),
                        "maxWorkerCount", command.getMaxWorkerCount()),

                // status
                "status", doc(
                        "assignmentCount", 0,
                        "hasWork", false,
                        "lastCheckedDate", MIN_DATE,
                        "dueDate", MIN_DATE,
                        "workerCount", 0,
                        "workers", new ArrayList<BasicDBObject>()));

        try {
            dal.getPartitionsCollection().insertOne(create);
        } catch (final MongoWriteException e) {
            if (e.getMessage().contains("E11000 duplicate key error collection")) {
                throw new PartitionExistsException(command.getPartitionKey());
            }
            throw e;
        }
    }

    @Override
    public void setPartitionStatus(SetPartitionStatusCommand command) {
        try {
            val updatePartition = doc("$set", doc(
                    "status.hasWork", command.isHasWork(),
                    "status.lastCheckedDate", new Date()));

            val find = doc("partitionKey", command.getPartitionKey());
            dal.getPartitionsCollection().updateOne(find, updatePartition);
        } catch (final Exception ex) {
            log.error("Error updating worker status.", ex);
        }
    }

    @Override
    public List<PartitionInfo> getPartitions() {
        val result = new ArrayList<PartitionInfo>();
        val partitions = dal.getPartitionsCollection().find().iterator();
        partitions.forEachRemaining(doc -> {
            // partition / workers
            val workers = new ArrayList<PartitionWorkerInfo>();
            val status = (Document) doc.get("status");
            val workersSource = (List<Document>) status.get("workers");
            workersSource.iterator().forEachRemaining(d -> workers.add(PartitionWorkerInfo
                    .builder()
                    .assignee(d.getString("assignee"))
                    .createDate(d.getDate("insertDate"))
                    .rule(d.getString("rule"))
                    .build()));

            // partition
            val configuration = (Document) doc.get("configuration");
            result.add(PartitionInfo
                    .builder()
                    .partitionKey(doc.getString("partitionKey"))
                    .currentWorkerCount(status.getInteger("workerCount"))
                    .hasMoreWork(status.getBoolean("hasWork"))
                    .lastChecked(status.getDate("lastCheckedDate"))
                    .maxIdleTimeSeconds(configuration.getInteger("maxIdleTimeSeconds"))
                    .maxWorkerCount(configuration.getInteger("maxWorkerCount"))
                    .workers(workers)
                    .build());
        });
        return result;
    }

    @Override
    public List<ConsumerInfo> getConsumers() {
        val result = new ArrayList<ConsumerInfo>();
        val consumers = dal.getConsumersCollection().find().iterator();
        consumers.forEachRemaining(doc -> {
            // consumer / workers
            val workers = new ArrayList<ConsumerWorkerInfo>();
            val status = (Document) doc.get("status");
            if (status != null) {
                val workersSource = (List<Document>) status.get("workers");
                workersSource.iterator().forEachRemaining(workerDoc -> {
                    val w = ConsumerWorkerInfo
                            .builder()
                            .workerNumber(workerDoc.getInteger("workerNumber"));
                    val assignmentDoc = (Document) workerDoc.get("assignment");
                    if (assignmentDoc != null) {
                        w
                                .assignmentDate(assignmentDoc.getDate("assignmentDate"))
                                .partitionKey(assignmentDoc.getString("partitionKey"))
                                .rule(assignmentDoc.getString("ruleName"));
                    }
                    workers.add(w.build());
                });
            }

            // consumer
            result.add(
                    ConsumerInfo
                            .builder()
                            .name(doc.getString("name"))
                            .connectedDate(doc.getDate("connectDate"))
                            .maxWorkerCount(doc.getInteger("maxWorkerCount"))
                            .workers(workers)
                            .build()
            );
        });
        return result;
    }

    /**
     * Retrieves and caches partition configuration objects.
     * They are cached for 5 minutes.
     *
     * @param partitionKey
     * @return
     */
    @Override
    public PartitionConfiguration getPartitionConfiguration(final String partitionKey) {
        return configurationCache.getConfiguration(partitionKey);
    }

    /**
     * Get an assignment for the executor.
     *
     * @param status
     * @return
     */
    public Assignment getAssignment(@NonNull final WorkerStatus status) {
        return assignmentStrategy.getAssignment(status);
    }

    /**
     * Release the assignment.
     *
     * @param command
     */
    @Override
    public void releaseAssignment(@NonNull ReleaseAssignmentCommand command) {
        assignmentStrategy.releaseAssignment(command.getAssignment());
    }

    /**
     * Register a consumer.
     *
     * @param command
     * @return
     * @throws ConsumerExistsException
     */
    @Override
    public ConsumerRegistration registerConsumer(final RegisterConsumerCommand command) throws ConsumerExistsException {
        // todo: use receipt
        val consumer =
                doc("name", command.getId().getName(),
                        "connectDate", new Date(),
                        "maxWorkerCount", command.getMaxWorkerCount(),
                        "status", null);

        try {
            dal.getConsumersCollection().insertOne(consumer);
        } catch (final MongoWriteException e) {
            if (e.getMessage().contains("E11000 duplicate key error collection")) {
                throw new ConsumerExistsException(command.getId().getName());
            }
            throw e;
        }
        return new ConsumerRegistration(command.getId(), "");
    }

    @Override
    public void updateConsumerStatus(final UpdateConsumerStatusCommand consumerStatus) {
        try {
            if (consumerStatus.getRegistration() == null) {
                // consumer isn't registered yet. nothing to save.
                return;
            }

            val find = doc("name", consumerStatus.getRegistration().getConsumerId().getName());
            val update = doc("$set", doc("status", consumerStatus.getStatus()));
            dal.getConsumersCollection().findOneAndUpdate(find, update);
        } catch (final Exception ex) {
            log.error("Error updating consumer status", ex);
        }
    }

    /**
     * Unregister the consumer.
     *
     * @param command
     */
    @Override
    public void unregisterConsumer(UnregisterConsumerCommand command) {
        // todo: use receipt
        val delete = doc("name", command.getRegistration().getConsumerId().getName());
        dal.getConsumersCollection().deleteOne(delete);
    }

    @Override
    public void close() {

    }
}
