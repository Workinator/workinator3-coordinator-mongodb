package com.allardworks.workinator3.coordinator.mongodb;

import com.allardworks.workinator3.core.*;
import com.allardworks.workinator3.core.commands.CreatePartitionCommand;
import com.allardworks.workinator3.core.commands.RegisterConsumerCommand;
import com.allardworks.workinator3.core.commands.ReleaseAssignmentCommand;
import com.allardworks.workinator3.core.commands.UnregisterConsumerCommand;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.stream.Collectors;

import static com.allardworks.workinator3.coordinator.mongodb.WhatsNextAssignmentStrategy.*;
import static java.lang.System.out;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/*

Use the WORKINATOR TEST HARNESS, as demonstrated in the A WHOLE BUNCH OF STUFF test.

TODO: most of these should move to WHATS NEXT ASSIGNMENT STRATEGY TESTS

 */

public abstract class WorkinatorTests {
    protected abstract WorkinatorTester getTester();

    private WorkerStatus createStatus(final String consumerId) {
        return new WorkerStatus(new WorkerId(new ConsumerRegistration(new ConsumerId(consumerId), ""), 1));
    }

    /**
     * Create and retrieve partitions.
     * <p>
     * This exercises the GET part, for which a bug was discovered on 3/25/2018.
     * BSON.DOCUMENT doeesn't support dot notation, so values in child objects (status.workers)
     * were coming back null.
     *
     * @throws Exception
     */
    @Test
    public void getPartition() throws Exception {
        try (val tester = new WorkinatorTestHarness(getTester())) {
            tester
                    .createPartition("a")
                    .createPartition("b")
                    .createWorker("a")
                    .createWorker("b")
                    .assertGetAssignment("a", "a", RULE1)
                    .assertGetAssignment("b", "b", RULE1);

            val partitions = tester.getTester().getWorkinator().getPartitions();
            assertEquals(2, partitions.size());
            assertEquals(1, partitions.get(0).getWorkers().size());
            assertEquals(1, partitions.get(1).getWorkers().size());
        }
    }

    @Test
    public void getConsumers() throws Exception {
        try (val tester = new WorkinatorTestHarness(getTester())) {
            tester
                    .createPartition("a")
                    .createPartition("b")
                    .createWorker("a")
                    .createWorker("b");

            val c1 = new ConsumerId("yadda");
            val c2 = new ConsumerId("dabba");
            tester.getTester().getWorkinator().registerConsumer(RegisterConsumerCommand.builder().id(c1).build());
            tester.getTester().getWorkinator().registerConsumer(RegisterConsumerCommand.builder().id(c2).build());

            val consumers = tester.getTester().getWorkinator().getConsumers();
            assertEquals(2, consumers.size());
        }
    }

    @Test
    public void doNotExceedMaxWorkerCount() throws Exception {
        val testSize = 10;

        try (val tester = getTester()) {
            try (val workinator = tester.getWorkinator()) {

                val partition = CreatePartitionCommand.builder().partitionKey("yadda").maxWorkerCount(testSize).build();
                workinator.createPartition(partition);

                val register = RegisterConsumerCommand.builder().id(new ConsumerId("smashing")).build();
                val registration = workinator.registerConsumer(register);

                tester.setHasWork("yadda", true);

                // will work for up to testSize.
                for (int i = 0; i < testSize; i++) {
                    val worker = new WorkerStatus(new WorkerId(registration, i));
                    val assignment = workinator.getAssignment(worker);
                    out.println(i);
                    assertEquals("yadda", assignment.getPartitionKey());
                }

                //next one will fail.
                val finalWorker = new WorkerStatus(new WorkerId(registration, testSize + 1));
                val finalAssignment = workinator.getAssignment(finalWorker);
                assertNull(finalAssignment);
            }
        }
    }

    @Test
    public void RULE2_getsSameAssignmentIfNothingElseAvailable_HasWork() throws Exception {
        try (val tester = getTester()) {
            try (val workinator = tester.getWorkinator()) {
                val partition = CreatePartitionCommand.builder().partitionKey("yadda").maxWorkerCount(1).build();
                workinator.createPartition(partition);

                val register = RegisterConsumerCommand.builder().id(new ConsumerId("smashing")).build();
                val registration = workinator.registerConsumer(register);

                val worker1 = new WorkerStatus(new WorkerId(registration, 1));
                val assignment1 = workinator.getAssignment(worker1);
                worker1.setCurrentAssignment(assignment1);
                assertEquals("yadda", assignment1.getPartitionKey());
                assertEquals(RULE1, assignment1.getRuleName());

                // nothing is due, and current assignment has work,
                // so keep with current assignment.
                throw new NotImplementedException();

                //worker1.setHasWork(true);
                //val assignment2 = workinator.getAssignment(worker1);
                //assertEquals(RULE2, assignment2.getRuleName());
                //assertEquals(assignment1.getPartitionKey(), assignment2.getPartitionKey());
            }
        }
    }

    @Test
    public void canGetAssignmentAfterItIsReleased() throws Exception {
        try (val tester = getTester()) {
            try (val workinator = tester.getWorkinator()) {
                val partition = CreatePartitionCommand.builder().partitionKey("yadda").maxWorkerCount(1).build();
                workinator.createPartition(partition);

                val register = RegisterConsumerCommand.builder().id(new ConsumerId("smashing")).build();
                val registration = workinator.registerConsumer(register);

                val worker1 = new WorkerStatus(new WorkerId(registration, 1));
                val assignment1 = workinator.getAssignment(worker1);
                worker1.setCurrentAssignment(assignment1);
                assertEquals("yadda", assignment1.getPartitionKey());

                // one partition already assigned, so nothing to do here.
                val worker2 = new WorkerStatus(new WorkerId(registration, 2));
                val assignment2 = workinator.getAssignment(worker2);
                assertNull(assignment2);

                // release the partition, then another worker can get it
                workinator.releaseAssignment(new ReleaseAssignmentCommand(assignment1));
                val assignment3 = workinator.getAssignment(worker2);
                assertEquals("yadda", assignment3.getPartitionKey());
            }
        }
    }

    @Test
    public void partitionCanOnlyBeCreatedOnce() throws Exception {
        try (val tester = getTester()) {
            try (val workinator = tester.getWorkinator()) {
                val partition = CreatePartitionCommand.builder().partitionKey("abc").build();
                workinator.createPartition(partition);

                try {
                    workinator.createPartition(partition);
                    Assert.fail("should've failed");
                } catch (final PartitionExistsException ex) {
                    assertEquals("abc", partition.getPartitionKey());
                }
            }
        }
    }

    @Test
    public void registerAndUnregisterConsumer() throws Exception {
        try (val tester = getTester()) {
            try (val workinator = tester.getWorkinator()) {
                val registerCommand = RegisterConsumerCommand.builder().id(new ConsumerId("boo")).build();

                // this works because not already registered
                val registration = workinator.registerConsumer(registerCommand);

                //  now it fails because already registered
                try {
                    workinator.registerConsumer(registerCommand);
                    Assert.fail("should've failed");
                } catch (final ConsumerExistsException ex) {
                    assertEquals("boo", ex.getConsumerId());
                }

                // unregister
                workinator.unregisterConsumer(new UnregisterConsumerCommand(registration));

                // now can register again
                workinator.registerConsumer(registerCommand);
            }
        }
    }

    /**
     * Get the first due.
     *
     * @throws Exception
     */
    @Test
    public void rule1() throws Exception {
        try (val tester = getTester()) {
            try (val workinator = tester.getWorkinator()) {
                val par1 = CreatePartitionCommand.builder().partitionKey("a").maxWorkerCount(5).build();
                workinator.createPartition(par1);

                val par2 = CreatePartitionCommand.builder().partitionKey("b").maxWorkerCount(5).build();
                workinator.createPartition(par2);

                val par3 = CreatePartitionCommand.builder().partitionKey("c").maxWorkerCount(5).build();
                workinator.createPartition(par3);

                tester.setDueDateFuture("a");
                tester.setDueDateFuture("c");

                val a1 = workinator.getAssignment(createStatus("zz"));
                assertEquals("b", a1.getPartitionKey());
                assertEquals(RULE1, a1.getRuleName());
            }
        }
    }

    /**
     * Get the first that is known to have work.
     *
     * @throws Exception
     */
    @Test
    public void rule3() throws Exception {
        try (val tester = getTester()) {
            try (val workinator = tester.getWorkinator()) {
                val par1 = CreatePartitionCommand.builder().partitionKey("a").maxWorkerCount(5).build();
                workinator.createPartition(par1);

                val par2 = CreatePartitionCommand.builder().partitionKey("b").maxWorkerCount(5).build();
                workinator.createPartition(par2);

                val par3 = CreatePartitionCommand.builder().partitionKey("c").maxWorkerCount(5).build();
                workinator.createPartition(par3);

                // a and c have work.b does not.
                // b will be the first one to get picked up.
                //tester.setDueDateFuture("a");
                tester.setHasWork("a", false);
                tester.setDueDateFuture("b");
                tester.setHasWork("b", true);
                tester.setDueDateFuture("c");
                tester.setHasWork("c", false);

                // TODO: fails because A has workercount=1
                // need to fix that
                val a1 = workinator.getAssignment(createStatus("zz"));
                assertEquals(RULE2, a1.getRuleName());
                assertEquals("b", a1.getPartitionKey());
                tester.setHasWork("b", false);

                val a2 = workinator.getAssignment(createStatus("zz"));
                assertEquals(RULE3, a2.getRuleName());
                assertEquals("a", a2.getPartitionKey());

                val a3 = workinator.getAssignment(createStatus("zz"));
                assertEquals("c", a3.getPartitionKey());
                assertEquals(RULE3, a3.getRuleName());
            }
        }
    }

    /**
     * None of the partitions are due, and none have work.
     * Thus Rule4 will take effect: the first partition
     *
     * @throws Exception
     */
    @Test
    public void rule4() throws Exception {
        try (val tester = getTester()) {
            try (val workinator = tester.getWorkinator()) {
                val par1 = CreatePartitionCommand.builder().partitionKey("a").maxWorkerCount(5).build();
                workinator.createPartition(par1);

                val par2 = CreatePartitionCommand.builder().partitionKey("b").maxWorkerCount(5).build();
                workinator.createPartition(par2);

                val par3 = CreatePartitionCommand.builder().partitionKey("c").maxWorkerCount(5).build();
                workinator.createPartition(par3);

                tester.setDueDateFuture("a");
                tester.setDueDateFuture("b");
                tester.setDueDateFuture("c");

                val a1 = workinator.getAssignment(createStatus("zz"));
                assertEquals("a", a1.getPartitionKey());
                assertEquals(RULE3, a1.getRuleName());

                val a2 = workinator.getAssignment(createStatus("zz"));
                assertEquals("b", a2.getPartitionKey());
                assertEquals(RULE3, a2.getRuleName());

                val a3 = workinator.getAssignment(createStatus("zz"));
                assertEquals("c", a3.getPartitionKey());
                assertEquals(RULE3, a3.getRuleName());
            }
        }
    }

    @Test
    public void rules1and3() throws Exception {
        try (val tester = getTester()) {
            try (val workinator = tester.getWorkinator()) {
                val par1 = CreatePartitionCommand.builder().partitionKey("a").maxWorkerCount(5).build();
                workinator.createPartition(par1);

                val par2 = CreatePartitionCommand.builder().partitionKey("b").maxWorkerCount(5).build();
                workinator.createPartition(par2);

                val par3 = CreatePartitionCommand.builder().partitionKey("c").maxWorkerCount(5).build();
                workinator.createPartition(par3);

                // 3 partitions. first 3 assignment all get the IsDue.

                val a1 = workinator.getAssignment(createStatus("consumer a"));
                assertEquals("a", a1.getPartitionKey());
                assertEquals(RULE1, a1.getRuleName());

                val a2 = workinator.getAssignment(createStatus("consumer b"));
                assertEquals("b", a2.getPartitionKey());
                assertEquals(RULE1, a2.getRuleName());

                val a3 = workinator.getAssignment(createStatus("consumer c"));
                assertEquals("c", a3.getPartitionKey());
                assertEquals(RULE1, a3.getRuleName());

                // -----------------------------------------------------------------
                // now we'll start getting rule 3. Rule 3
                // assigns threads to partitions that are being worked on and
                // can support more threads.
                // -----------------------------------------------------------------

                // set the partitions to haswork=true so that rule 4 takes effect.
                tester.setHasWork("a", true);
                tester.setHasWork("b", true);
                tester.setHasWork("c", true);

                val a4 = workinator.getAssignment(createStatus("consumer a"));
                assertEquals("a", a4.getPartitionKey());
                assertEquals(RULE2, a4.getRuleName());

                val a5 = workinator.getAssignment(createStatus("consumer b"));
                assertEquals("b", a5.getPartitionKey());
                assertEquals(RULE2, a5.getRuleName());

                val a6 = workinator.getAssignment(createStatus("consumer c"));
                assertEquals("c", a6.getPartitionKey());
                assertEquals(RULE2, a6.getRuleName());

                // release one, then get an assignment
                // we'll get the same one back because rule 3 will see it has the fewest workers
                workinator.releaseAssignment(new ReleaseAssignmentCommand(a2));
                val a7 = workinator.getAssignment(createStatus("consumer b"));
                assertEquals("b", a7.getPartitionKey());
                assertEquals(RULE2, a7.getRuleName());
            }
        }
    }

    @Test
    public void RULE2_multipleConcurrency() throws Exception {
        try (val tester = new WorkinatorTestHarness(getTester())) {
            tester
                    // setup the partition and 4 workers
                    .createPartition("aaa", 3)
                    .createWorker("worker a")
                    .createWorker("worker b")
                    .createWorker("worker c")
                    .createWorker("worker d")

                    // get worker a then save it.
                    // this will udpate the partition with hasWork=true, which is necessary
                    // in order for subsequent workers to be assigned to the same partition.
                    .assertGetAssignment("worker a", "aaa", RULE1)
                    .setWorkerHasWork("worker a")
                    .saveWorkersStatus()

                    // b and c will be assigned to the same partition.
                    .assertGetAssignment("worker b", "aaa", RULE2)
                    .assertGetAssignment("worker c", "aaa", RULE2)

                    // max concurrency reached.
                    // next worker won't get an assignment.
                    .assertNullAssignment("worker d");
        }
    }

    @Test
    public void RULE2_MultipleConcurrency() throws Exception {
        try (val tester = new WorkinatorTestHarness(getTester())) {
            tester
                    // setup the partition and 4 workers
                    .createPartition("aaa", 3)
                    .createPartition("bbb", 3)
                    .setPartitionHasWork("aaa")
                    .setPartitionHasWork("bbb")
                    .createWorker("worker a")
                    .createWorker("worker b")
                    .createWorker("worker c")
                    .createWorker("worker d")
                    .createWorker("worker e")
                    .createWorker("worker f")
                    .createWorker("worker g")

                    .assertGetAssignment("worker a", "aaa", RULE1)
                    .assertGetAssignment("worker b", "bbb", RULE1)
                    .assertGetAssignment("worker c", "aaa", RULE2)
                    .assertGetAssignment("worker d", "bbb", RULE2)
                    .assertGetAssignment("worker e", "aaa", RULE2)
                    .assertGetAssignment("worker f", "bbb", RULE2)

                    // max concurrency reached for both partitions
                    // next worker won't get an assignment.
                    .assertNullAssignment("worker g");
        }
    }

    /**
     * New partitions always get priority.
     *
     * @throws Exception
     */
    @Test
    public void RULE2_MultipleConcurrencyAcrossPartitions_TrumpedByRule1() throws Exception {
        try (val tester = new WorkinatorTestHarness(getTester())) {
            tester
                    // setup the partition and 4 workers
                    .createPartition("aaa", 3)
                    .createPartition("bbb", 3)
                    .setPartitionHasWork("aaa")
                    .setPartitionHasWork("bbb")
                    .createWorker("worker a")
                    .createWorker("worker b")
                    .createWorker("worker c")
                    .createWorker("worker d")
                    .createWorker("worker e")
                    .createWorker("worker f")
                    .createWorker("worker g")

                    // assignments will alternate
                    .assertGetAssignment("worker a", "aaa", RULE1)
                    .assertGetAssignment("worker b", "bbb", RULE1)
                    .assertGetAssignment("worker c", "aaa", RULE2)
                    .assertGetAssignment("worker d", "bbb", RULE2)

                    // now create a new partition. it will get priority.
                    .createPartition("ccc")
                    .assertGetAssignment("worker e", "ccc", RULE1)

                    // now back to the others, which have work and multiple workeres
                    .assertGetAssignment("worker f", "aaa", RULE2)
                    .assertGetAssignment("worker g", "bbb", RULE2);
        }
    }

    /**
     * A partition is busy with max workers = 10.
     * A single consumer with 10 workers will support that.
     * If a second partition is created with max workers = 10, and is buy,
     * then the consumer should dedicate 5 threads to each.
     * <p>
     * 4/20/2018 - this doesn't work.
     */
    @Test
    public void shouldBalanceWhenMultiplePartitionsMayHaveMultipleWorkers() throws Exception {
        try (val tester = new WorkinatorTestHarness(getTester())) {
            tester
                    // create 3 partitions
                    .createPartition("a", 10)
                    .setPartitionHasWork("a")
                    .createWorkers("worker a", "worker b", "worker c", "worker d", "worker e", "worker f", "worker g", "worker h", "worker i", "worker j")
                    .assertGetAssignment("worker a", "a", RULE1)
                    .assertGetAssignment("worker b", "a", RULE2)
                    .assertGetAssignment("worker c", "a", RULE2)
                    .assertGetAssignment("worker d", "a", RULE2)
                    .assertGetAssignment("worker e", "a", RULE2)
                    .assertGetAssignment("worker f", "a", RULE2)
                    .assertGetAssignment("worker g", "a", RULE2)
                    .assertGetAssignment("worker h", "a", RULE2)
                    .assertGetAssignment("worker i", "a", RULE2)
                    .assertGetAssignment("worker j", "a", RULE2)
                    .setWorkersHaveWork("worker a", "worker b", "worker c", "worker d", "worker e", "worker f", "worker g", "worker h", "worker i", "worker j")

                    .createPartition("b", 10)
                    .setPartitionHasWork("b")

                    // 5 to b
                    .assertGetAssignment("worker a", "b", RULE1)
                    .assertGetAssignment("worker b", "b", RULE2)
                    .assertGetAssignment("worker c", "b", RULE2)
                    .assertGetAssignment("worker d", "b", RULE2)
                    .assertGetAssignment("worker e", "b", RULE2)

                    // and still 5 allocated to a
                    .assertGetAssignment("worker f", "a", RULE2)
                    .assertGetAssignment("worker g", "a", RULE2)
                    .assertGetAssignment("worker h", "a", RULE2)
                    .assertGetAssignment("worker i", "a", RULE2)
                    .assertGetAssignment("worker j", "a", RULE2)

                    // -----------------------------------------------------------------------
                    // add partition c with work and 2 workers
                    // will end up with c = 2, a = 4, b = 4
                    // -----------------------------------------------------------------------
                    .createPartition("c", 2)
                    .setPartitionHasWork("c")
                    .assertGetAssignment("worker a", "c", RULE1)
                    .assertGetAssignment("worker b", "c", RULE2)

                    .assertGetAssignment("worker c", "b", RULE2)
                    .assertGetAssignment("worker d", "b", RULE2)
                    .assertGetAssignment("worker e", "b", RULE2)
                    .assertGetAssignment("worker j", "b", RULE2)

                    .assertGetAssignment("worker f", "a", RULE2)
                    .assertGetAssignment("worker g", "a", RULE2)
                    .assertGetAssignment("worker h", "a", RULE2)
                    .assertGetAssignment("worker i", "a", RULE2)

                    // -----------------------------------------------------------------------
                    // add partitions d and e with work and 1 workers each.
                    // a = 3, b = 3, c = 2, d = 1, e = 1
                    // -----------------------------------------------------------------------
                    .createPartition("d", 1)
                    .createPartition("e", 1)
                    .setPartitionHasWork("d")
                    .setPartitionHasWork("e")

                    // these are new. never did work before, so highest priority.
                    .assertGetAssignment("worker a", "d", RULE1)
                    .assertGetAssignment("worker b", "e", RULE1)

                    // these don't have any workers, but we know they have work,
                    // so they're next
                    .assertGetAssignment("worker c", "c", RULE1)
                    .assertGetAssignment("worker d", "c", RULE2)

                    // this isn't fully explored. we could explain why each of these result
                    // in their assignments. in general, though, by the time it's done,
                    // all partitions have the proper number of workers.
                    .assertGetAssignment("worker e", "b", RULE2)
                    .assertGetAssignment("worker f", "b", RULE2)
                    .assertGetAssignment("worker g", "a", RULE2)
                    .assertGetAssignment("worker h", "a", RULE2)
                    .assertGetAssignment("worker i", "a", RULE2)
                    .assertGetAssignment("worker j", "b", RULE2);

        }
    }

    @Test
    public void rulezzz() throws Exception {
        try (val tester = new WorkinatorTestHarness(getTester())) {
            tester
                    .createPartition("a", 10)
                    .createPartition("b", 10)
                    .createPartition("c", 4)
                    .setPartitionHasWork("a")
                    .setPartitionHasWork("b")
                    .setPartitionHasWork("c");

            // consumer has 20 workers
            // should end up with a = 8, b = 8, c = 4
            val reg = new ConsumerRegistration(new ConsumerId("a"), "");
            for (int i = 0; i < 20; i++) {
                val status = new WorkerStatus(new WorkerId(reg, i));
                tester.getTester().getWorkinator().getAssignment(status);
            }

            val partitions = tester.getTester().getWorkinator().getPartitions();
            for (val p : partitions) {
                out.println("--------------------\n" + p.getPartitionKey() + "\n------------------------");
                for (val w : p.getWorkers()) {
                    out.println(w.getAssignee());
                }
            }
        }
    }

    /**
     * Get an assignment and release it.
     * Assure that the workers increase to 1 then decrease to 0.
     * 4/22/2018: i changed the assignment id to be based on receipt. As I worked on that,
     * I saw that no tests were failing. Thus, tests for release are lacking.
     * This is a very basic test, but shows that the RECEIPT approach is working.
     *
     * @throws Exception
     */
    @Test
    public void release() throws Exception {
        try (val tester = new WorkinatorTestHarness(getTester())) {
            tester.createPartition("a");

            val workerStatus = new WorkerStatus(new WorkerId(new ConsumerRegistration(new ConsumerId("boo"), ""), 1));
            val assignment = tester.getTester().getWorkinator().getAssignment(workerStatus);

            {
                val partitions1 = tester.getTester().getWorkinator().getPartitions();
                assertEquals(1, partitions1.size());
                assertEquals(1, partitions1.get(0).getWorkers().size());
            }
            tester.getTester().getWorkinator().releaseAssignment(new ReleaseAssignmentCommand(assignment));

            {
                val partitions2 = tester.getTester().getWorkinator().getPartitions();
                assertEquals(1, partitions2.size());
                assertEquals(0, partitions2.get(0).getWorkers().size());
            }
        }
    }

    @Test
    public void AWholeBunchOfStuff() throws Exception {
        try (val tester = new WorkinatorTestHarness(getTester())) {
            tester
                    // create 3 partitions
                    .createPartition("a")
                    .createPartition("b")
                    .createPartition("c")

                    // ---------------------------------------------
                    // Rule 1
                    // ---------------------------------------------

                    // partitions will be returned in the order created
                    .createWorker("worker a")
                    .assertGetAssignment("worker a", "a", RULE1)
                    .createWorker("worker b")
                    .assertGetAssignment("worker b", "b", RULE1)
                    .createWorker("worker c")
                    .assertGetAssignment("worker c", "c", RULE1)

                    // nothing for the 4th worker to do.
                    // all partitions have max concurrency = 1, and there are now more workers than partitions
                    .createWorker("worker d")
                    .assertNullAssignment("worker d")

                    // release the worker b
                    .releaseAssignment("worker b")

                    // ---------------------------------------------
                    // RULE 4
                    // ---------------------------------------------
                    // now d will get it
                    // RULE 4 because it's not due and doesn't have work.
                    // RULE 4 is first partition where worker count = 0
                    .assertGetAssignment("worker d", "b", RULE3)

                    // ---------------------------------------------
                    // RULE 2
                    // ---------------------------------------------
                    // a has work, and there aren't any partitions due
                    // getting assignment will result in rule 2
                    .setWorkerHasWork("worker a")
                    .assertGetAssignment("worker a", "a", RULE2)

                    // ---------------------------------------------
                    // RULE 5
                    // ---------------------------------------------
                    // c doesn't have work, and there aren't any partitions due.
                    // RULE2 and RULE2 are for when there is work. there isn't.
                    // RULE3 won't find this partition because it's still assigned.
                    // RULE4 will prevail... nothing better to do, so do what you're doing
                    // even though there isn't work.
                    .setWorkerDoesntHaveWork("worker c")
                    .assertGetAssignment("worker c", "c", RULE4);
        }
    }
}
