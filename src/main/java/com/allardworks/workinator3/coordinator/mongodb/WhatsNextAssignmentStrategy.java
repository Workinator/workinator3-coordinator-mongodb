package com.allardworks.workinator3.coordinator.mongodb;

import com.allardworks.workinator3.coordinator.AssignmentStrategy;
import com.allardworks.workinator3.core.Assignment;
import com.allardworks.workinator3.core.WorkerStatus;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.apache.commons.lang3.time.DateUtils;
import org.bson.Document;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.function.Supplier;

import static com.allardworks.workinator3.coordinator.mongodb.DocumentUtility.doc;
import static com.mongodb.client.model.ReturnDocument.AFTER;

/**
 * Assignment strategy that determines a worker's assignment.
 */
@Component
@RequiredArgsConstructor
public class WhatsNextAssignmentStrategy implements AssignmentStrategy {
    public final static String RULE1 = "Rule 1 - Not being worked on, and is due to be checked.";
    public final static String RULE2 = "Rule 2 - Already busy, so keep going.";
    public final static String RULE3 = "Rule 3 - Partition already being worked on, but supports more workers.";
    public final static String RULE4 = "Rule 4 - Any partition that doesn't have a worker, even if it's not due yet.";
    public final static String RULE5 = "Rule 5 - Nothing else to do, so might as well keep working with the current assignment.";

    private final FindOneAndUpdateOptions updateOptions = new FindOneAndUpdateOptions()
            .returnDocument(AFTER)
            .sort(doc("status.lastCheckedDate", 1));

    private final MongoDal dal;
    private final PartitionConfigurationCache partitionConfigurationCache;
    private final FindOneAndUpdateOptions releaseOptions = new FindOneAndUpdateOptions().projection(doc("_id", 1));

    /**
     * The WHERE for rule #3.
     * All documents that have work and capacity for more workers.
     */
    private final Document alreadyBeingWorkedOnFilter = Document.parse("{ $and: [ { \"status.hasWork\": true }, { \"status.workerCount\": { \"$gt\": 0 } }, { $expr :  { $lt: [ \"$status.workerCount\", \"$configuration.maxWorkerCount\" ] } } ] }");

    /**
     * The UPDATE options for rule #3.
     * Order by the worker count and the date.
     */
    private final FindOneAndUpdateOptions alreadyBeingWorkedOnUpdateOptions =  new FindOneAndUpdateOptions()
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
                                doc("assignee", assignment.getWorkerId().getAssignee())),
                        "$inc", doc("status.workerCount", -1),
                        "$set", doc("status.lastCheckedDate",
                                new Date(), "status.dueDate", dueDate));
        val result = dal.getPartitionsCollection().findOneAndUpdate(findPartition, removeWorker, releaseOptions);
    }

    @RequiredArgsConstructor
    private static class StrategyRunner {
        private final WhatsNextAssignmentStrategy strategy;
        private final WorkerStatus status;

        /**
         * The list of methods to execute to determine the assignment.
         * First one wins.
         */
        private final List<Supplier<Assignment>> rules = Arrays.asList(
                this::due,
                this::ifBusyKeepGoing,
                this::alreadyBeingWorkedOn,
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
                                    "rule", ruleName)),
                    "$inc", doc("status.workerCount", 1, "status.assignmentCount", 1),
                    "$set", doc("status.lastCheckedDate", new Date()));
        }

        /**
         * Converts a partition BSON document to an assignment object.
         * @param partition
         * @param status
         * @param ruleName
         * @return
         */
        private Assignment toAssignment(final Document partition, final WorkerStatus status, final String ruleName) {
            if (partition == null) {
                return null;
            }

            return new Assignment(status.getWorkerId(), partition.getString("partitionKey"), "", ruleName, new Date());
        }

        /**
         * RULE 1
         * Get the a partition that is due to be processed, and doesn't have any
         * current workers.
         *
         * This is the highest priority. Partitions have a maxIdleTime setting.
         * This enforces that those partitions that are due will have the highest priority.
         *
         * @return
         */
        private Assignment due() {
            val where = doc("status.workerCount", 0, "status.dueDate", doc("$lt", new Date()));
            val update = createWorkerUpdateDocument(RULE1);
            return toAssignment(strategy.dal.getPartitionsCollection()
                    .findOneAndUpdate(where, update, strategy.updateOptions), status, RULE1);
        }

        /**
         * RULE 2
         * If the executor is already hasWork, then let it keep doing what it's doing.
         *
         * @return
         */
        private Assignment ifBusyKeepGoing() {
            // TODO: update db with new rule
            if (status.isHasWork()) {
                return Assignment.setRule(status.getCurrentAssignment(), RULE2);
            }

            return null;
        }

        /**
         * RULE 3
         * Return a partition that is already being worked on.
         * Partitions that are being worked on, but support multiple workers.
         * @return
         */
        private Assignment alreadyBeingWorkedOn() {
            val update = createWorkerUpdateDocument(RULE3);
            return toAssignment(strategy.dal.getPartitionsCollection().findOneAndUpdate(strategy.alreadyBeingWorkedOnFilter, update, strategy.alreadyBeingWorkedOnUpdateOptions), status, RULE3);
        }

        /**
         * Rule 4
         * Find any partition that doesn't have a worker, even if it's not due yet.
         * If we have capacity to do work, might as well even though the partition isn't due.
         */
        private Assignment anyPartitionWithoutWorkers() {
            val update = createWorkerUpdateDocument(RULE4);
            return toAssignment(strategy.dal.getPartitionsCollection().findOneAndUpdate(strategy.noWorkers, update, strategy.updateOptions), status, RULE4);
        }

        /**
         * Rule 5
         * If there's nothing else to do, then continue with what it was already doing.
         * This differs from rule 2 in that rule 2 is matched if the worker has work.
         * Here, it doens't, but keep going anyway.
         * TODO: we could make this optional or drop it altogether.
         * @return
         */
        private Assignment existingAssignment() {
            return
                    status.getCurrentAssignment() == null
                            ? null
                            : Assignment.setRule(status.getCurrentAssignment(), RULE5);
        }

        /**
         * Determines the assignment for the worker.
         * Iterates a list of rules until one returns an assignment.
         * If no rule returns an assignment, then there's nothing to do.
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
