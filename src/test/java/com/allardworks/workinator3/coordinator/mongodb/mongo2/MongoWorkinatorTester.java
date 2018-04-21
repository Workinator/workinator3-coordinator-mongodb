package com.allardworks.workinator3.coordinator.mongodb.mongo2;

import com.allardworks.workinator3.coordinator.mongodb.*;
import com.allardworks.workinator3.core.Workinator;
import lombok.val;

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import static com.allardworks.workinator3.coordinator.mongodb.DocumentUtility.doc;

public class MongoWorkinatorTester implements WorkinatorTester {
    private MongoDal dal;

    private final Workinator workinator;

    public MongoWorkinatorTester() {
        val config = new MongoConfiguration();
        config.setDatabaseName("test");
        dal = new MongoDal(config);
        val cache = new PartitionConfigurationCache(dal);
        workinator = new MongoWorkinator(dal, cache, new WhatsNextAssignmentStrategy(dal, cache));
    }

    @Override
    public Workinator getWorkinator() {
        return workinator;
    }

    public void setHasWork(final String partitionKey, final boolean hasWork) {
        val find = doc("partitionKey", partitionKey);
        val update = doc("$set", doc("status.hasWork", hasWork));
        dal.getPartitionsCollection().findOneAndUpdate(find, update);
    }

    public void setDueDate(final String partitionKey, final Date dueDate) {
        val find = doc("partitionKey", partitionKey);
        val update = doc("$set", doc("status.dueDate", dueDate));
        dal.getPartitionsCollection().findOneAndUpdate(find, update);
    }

    public void setDueDateFuture(final String partitionKey) {
        val future = new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime();
        setDueDate(partitionKey, future);
    }

    @Override
    public void close() {
        dal.getDatabase().drop();
    }
}
