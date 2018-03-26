package com.allardworks.workinator3.coordinator.mongodb.mongo2;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import com.allardworks.workinator3.coordinator.mongodb.*;
import com.allardworks.workinator3.core.*;
import com.allardworks.workinator3.core.commands.CreatePartitionCommand;
import com.allardworks.workinator3.core.commands.ReleaseAssignmentCommand;
import lombok.val;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

public class PsrTests {
    final int partitionCount = 25000;

    /*
    -- before partition configuration cache
        create partitions: 4415ms
        get assignments: 6896ms
        release assignments: 5091ms
     */
    @Ignore
    @Test
    public void Rule1() throws Exception {
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        Logger rootLogger = loggerContext.getLogger("org.mongodb.driver");
        rootLogger.setLevel(Level.OFF);

        val dal = new MongoDal(MongoConfiguration.builder().databaseName("test").build());
        val cache = new PartitionConfigurationCache(dal);
        val workinator = new MongoWorkinator(dal, cache, new WhatsNextAssignmentStrategy(dal, cache));
        val assignments = new ArrayList<Assignment>();

        try (val timer = new TimedActivity("create partitions")) {
            for (int i = 0; i < partitionCount; i++) {
                val create = CreatePartitionCommand.builder().partitionKey("p-" + i).maxIdleTimeSeconds(10).maxWorkerCount(10).build();
                workinator.createPartition(create);
            }
        }

        // warm up the cache - preload all config objects
        try (val timer = new TimedActivity("warm up config cache")) {
            for (int i = 0; i < partitionCount; i++) {
                workinator.getPartitionConfiguration("p-" + i);
            }
        }

        try (val timer = new TimedActivity("get assignments")) {
            for (int i = 0; i < partitionCount; i++) {
                val workerStatus = new WorkerStatus(new WorkerId(new ConsumerRegistration(new ConsumerId("ca"), ""), 1));
                val assignment = workinator.getAssignment(workerStatus);
                assignments.add(assignment);
            }
        }

        try (val timer = new TimedActivity("release assignments")) {
            for (val a : assignments) {
                workinator.releaseAssignment(new ReleaseAssignmentCommand(a));
            }
        }
    }
}
