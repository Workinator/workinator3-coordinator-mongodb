package com.allardworks.workinator3.coordinator.mongodb;

import com.allardworks.workinator3.coordinator.AssignmentStrategy;
import com.allardworks.workinator3.core.Assignment;
import com.allardworks.workinator3.core.WorkerStatus;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import lombok.*;
import org.apache.commons.lang3.time.DateUtils;
import org.bson.BsonArray;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.Document;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.StreamSupport;

import static com.allardworks.workinator3.coordinator.mongodb.DocumentUtility.doc;
import static com.allardworks.workinator3.coordinator.mongodb.DocumentUtility.list;
import static com.mongodb.client.model.ReturnDocument.AFTER;
import static java.util.stream.Collectors.toList;

/**
 * Assignment strategy that determines a worker's assignment.
 */
@Component
@RequiredArgsConstructor
public class WhatsNextAssignmentStrategy implements AssignmentStrategy {
    public final static String RULE1 = "Rule 1 - Not being worked on, and is due to be checked.";
    public final static String RULE2 = "Rule 2 - Partition already being worked on, but supports more workers.";
    public final static String RULE3 = "Rule 3 - Any partition that doesn't have a worker, even if it's not due yet.";
    public final static String RULE4 = "Rule 4 - Nothing else to do, so might as well keep working with the current assignment.";

    private final FindOneAndUpdateOptions updateOptions = new FindOneAndUpdateOptions()
            .returnDocument(AFTER)
            .sort(doc("status.lastCheckedDate", 1));

    private final MongoDal dal;
    private final PartitionConfigurationCache partitionConfigurationCache;
    private final FindOneAndUpdateOptions releaseOptions = new FindOneAndUpdateOptions().projection(doc("_id", 1));

    /**
     * The UPDATE options for rule #3.
     * Order by the worker count and the date.
     */
    private final FindOneAndUpdateOptions alreadyBeingWorkedOnUpdateOptions = new FindOneAndUpdateOptions()
            .returnDocument(AFTER)
            .sort(doc("status.workerCount", 1, "status.lastCheckedDate", 1));

    /**
     * The WHERE for rule #4.
     * An partition that doesn't have any workers.
     */
    private final Document noWorkers = doc("status.workerCount", 0);


    @Override
    public Assignment getAssignment(@NonNull final WorkerStatus status) {
        return new StrategyRunner(this, status).getAssignment();
    }

    @Override
    public void releaseAssignment(final Assignment assignment) {
        // due date is now + maxIdleTimeSeconds.
        val dueDate = DateUtils.addSeconds(Date.from(Instant.now()), partitionConfigurationCache.getConfiguration(assignment.getPartitionKey()).getMaxIdleTimeSeconds());
        val findPartition = doc("partitionKey", assignment.getPartitionKey());
        val removeWorker =
                doc("$pull",
                        doc("status.workers",
                                doc(/*"assignee", assignment.getWorkerId().getAssignee(), */"receipt", UUID.fromString(assignment.getReceipt()))),
                        "$inc", doc("status.workerCount", -1),
                        "$set", doc("status.lastCheckedDate",
                                new Date(), "status.dueDate", dueDate));
        val result = dal.getPartitionsCollection().findOneAndUpdate(findPartition, removeWorker, releaseOptions);
    }

    @RequiredArgsConstructor
    private static class StrategyRunner {
        private final WhatsNextAssignmentStrategy strategy;
        private final WorkerStatus status;
        private final UUID receipt = UUID.randomUUID();

        /**
         * The list of methods to execute to determine the assignment.
         * First one wins.
         */
        private final List<Supplier<Assignment>> rules = Arrays.asList(
                this::dueOrHasWork,
                this::multipleWorkers,
                this::anyPartitionWithoutWorkers,
                this::existingAssignment);

        /**
         * Creates the update document. Most rules need this.
         *
         * @param ruleName
         * @return
         */
        private Document createWorkerUpdateDocument(final String ruleName) {
            return doc("$push",
                    doc("status.workers",
                            doc("assignee", status.getWorkerId().getAssignee(),
                                    "insertDate", new Date(),
                                    "receipt", receipt,
                                    "rule", ruleName)),
                    "$inc", doc("status.workerCount", 1, "status.assignmentCount", 1),
                    "$set", doc("status.lastCheckedDate", new Date()));
        }

        /**
         * Converts a partition BSON document to an assignment object.
         *
         * @param partition
         * @param status
         * @param ruleName
         * @return
         */
        private Assignment toAssignment(final Document partition, final WorkerStatus status, final String ruleName) {
            if (partition == null) {
                return null;
            }

            return new Assignment(status.getWorkerId(), partition.getString("partitionKey"), receipt.toString(), ruleName, new Date());
        }

        private Document getDueOrHasWork() {
            return doc(
                    "status.workerCount", 0,
                    "$or", list(
                            doc("status.dueDate", doc("$lt", new Date())),
                            doc("status.hasWork", true)
                    )
            );
        }

        /**
         * RULE 1
         * Get the a partition that is due to be processed, and doesn't have any
         * current workers.
         * <p>
         * This is the highest priority. Partitions have a maxIdleTime setting.
         * This enforces that those partitions that are due will have the highest priority.
         *
         * @return
         */
        private Assignment dueOrHasWork() {
            val update = createWorkerUpdateDocument(RULE1);
            return toAssignment(strategy.dal.getPartitionsCollection()
                    .findOneAndUpdate(getDueOrHasWork(), update, strategy.updateOptions), status, RULE1);
        }

        @RequiredArgsConstructor
        @Getter
        private static class Match {
            private final String partitionKey;
            private final int workerCount;
        }

        private final String workerFilter = "{ $and: [ !!PARTITION!! { \"status.hasWork\": true }, { \"status.workerCount\": { \"$gt\": 0 } }, { $expr :  { $lt: [ \"$status.workerCount\", \"$configuration.maxWorkerCount\" ] } } ] }";

        private String determinePartitionToContinueWorkingOn() {
            val partition = status == null || status.getCurrentAssignment() == null ? "" : status.getCurrentAssignment().getPartitionKey();
            val query = BsonArray.parse("[\n" +
                    "    { $project: \n" +
                    "        { \n" +
                    "            id: 1, \n" +
                    "            partitionKey: 1, \n" +
                    "            status: 1,\n" +
                    "            createDate: 1,\n" +
                    "            configuration: 1,\n" +
                    "            sort: {\n" +
                    "                $cond: {\n" +
                    "                    if: {\n" +
                    "                        $eq: [\"$partitionKey\", \"" + partition + "\"]\n" +
                    "                    },\n" +
                    "                    then: 0,\n" +
                    "                    else: 1\n" +
                    "                }\n" +
                    "            }\n" +
                    "        }\n" +
                    "    }, \n" +
                    "    {\n" +
                    "       $sort: {\n" +
                    "           sort: 1,\n" +
                    "           \"status.workerCount\": 1,\n" +
                    "           \"status.lastCheckedDate\": 1\n" +
                    "       }\n" +
                    "    }," +
                    "    {" +
                    "       $match: " + workerFilter.replace("!!PARTITION!!", "") +
                    "    }," +
                    "    {" +
                    "       $limit: 2" +
                    "    }," +
                    "]");

            val docs = query.getValues().stream().map(d -> (BsonDocument) d).collect(toList());
            val currentPartitionKey =
                    status != null && status.getCurrentAssignment() != null
                            ? status.getCurrentAssignment().getPartitionKey()
                            : "";
            val match = strategy.dal.getPartitionsCollection().aggregate(docs);
            List<Match> values = StreamSupport.stream(match.spliterator(), false)
                    .map(d -> {
                        val p = d.getString("partitionKey");
                        val currentWorkerCount = ((Document) d.get("status")).getInteger("workerCount");
                        return new Match(p, currentWorkerCount);
                    })
                    .collect(toList());

            // no matches, nothing to do
            if (values.size() == 0) {
                return null;
            }

            // if there's only one match, then that's the one we need to go with.
            if (values.size() == 1) {
                return values.get(0).getPartitionKey();
            }

            // if the first one isn't the same as the current, then use the first one.
            if (!currentPartitionKey.equals(values.get(0).getPartitionKey())) {
                return values.get(0).getPartitionKey();
            }

            // prevent flip flop
            if (values.get(0).getWorkerCount() == values.get(1).getWorkerCount() + 1) {
                // if the current has 5 workers, and the next one has 4 workers,
                // then keep use the current one.
                // this is the situation that makes this complexity necessary.
                // if partition A has 5 workers and partition B has 4 workers, then without
                // this code, they would switch. A would have 4 then B would have 5. Then the next
                // check would revers them again, and on and on.
                // this prevents the flip-flop.
                return values.get(0).getPartitionKey();
            }

            // prevent flip flop
            if (values.get(0).getWorkerCount() == values.get(1).getWorkerCount()) {
                // similar to flip flop.
                // A has 5 and B has 5. But, they both have capacity for 10.
                // without this, then A would go to 4 and B would go to 6.
                // then on the next pass, they'd go back to 5 and 5... and on and on and on.
                // if they have the same number of workers, then don't reallocate.
                return values.get(0).getPartitionKey();
            }

            // return the one that has the fewest workers.
            // flip-flop has already been eliminated.
            return
                    values.get(0).getWorkerCount() < values.get(1).getWorkerCount()
                            ? values.get(0).getPartitionKey()
                            : values.get(1).getPartitionKey();
        }

        /**
         * RULE 2
         * Return a partition that is already being worked on.
         * Partitions that are being worked on, but support multiple workers.
         * <p>
         * This requires up 2 db operations per attempt.
         * The first is a read to get some information about the partitions.
         * The information is analyzed to determine which partition to assign.
         * If a different partition is chosen, then the UPDATE is executed to claim the partition.
         * If the claim fails because another thread has already done the same, then it will
         * loop back and start over.
         *
         * @return
         */
        private Assignment multipleWorkers() {

            while (true) {
                val partitionKey = determinePartitionToContinueWorkingOn();
                if (partitionKey == null) {
                    return null;
                }

                // if the assignment hasn't changed, then return the current.
                if (status != null && status.getCurrentAssignment() != null && status.getCurrentAssignment().getPartitionKey().equals(partitionKey)) {
                    return status.getCurrentAssignment();
                }

                // mostly the same as the initial filter, but now with the partitionKey filter.
                val filter2 = workerFilter.replace("!!PARTITION!!", "{\"partitionKey\": \"" + partitionKey + "\"}, ");
                val update = createWorkerUpdateDocument(RULE2);
                val result = strategy.dal.getPartitionsCollection().findOneAndUpdate(Document.parse(filter2), update, strategy.alreadyBeingWorkedOnUpdateOptions);
                if (result == null) {
                    // things may change between the QUERY and the UPDATE.
                    // if so, the update won't do anything, so loop back and try again.
                    continue;
                }
                return toAssignment(result, status, RULE2);
            }

        }

        /**
         * Rule 3
         * Find any partition that doesn't have a worker, even if it's not due yet.
         * If we have capacity to do work, might as well even though the partition isn't due.
         */
        private Assignment anyPartitionWithoutWorkers() {
            val update = createWorkerUpdateDocument(RULE3);
            return toAssignment(strategy.dal.getPartitionsCollection().findOneAndUpdate(strategy.noWorkers, update, strategy.updateOptions), status, RULE3);
        }

        /**
         * Rule 4
         * If there's nothing else to do, then continue with what it was already doing.
         *
         * @return
         */
        private Assignment existingAssignment() {
            return
                    status.getCurrentAssignment() == null
                            ? null
                            : Assignment.setRule(status.getCurrentAssignment(), RULE4);
        }

        /**
         * Determines the assignment for the worker.
         * Iterates a list of rules until one returns an assignment.
         * If no rule returns an assignment, then there's nothing to do.
         *
         * @return
         */
        public Assignment getAssignment() {
            for (val rule : rules) {
                val assignment = rule.get();
                if (assignment != null) {
                    return assignment;
                }
            }

            // nothing to do. capacity exceeds the number of partitions.
            return null;
        }
    }
}