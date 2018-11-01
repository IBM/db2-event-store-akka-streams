/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
/* Original work from Lightbend has been modified, as needed, and
 * continues to be Apache v2 licensed. Modifications include:
 * - licensing/copyright for derived work modifications below
 * - only ignore the negative tests (they cause conflicts)
 * - updated endpoints
 * - fixes to allow tests to run w/ existing databases/tables
 */
/*
 * Copyright 2018 IBM Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package akka.stream.alpakka.ibm.eventstore.javadsl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import scala.collection.JavaConversions;
import scala.collection.Seq;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.testkit.JavaTestKit;
import com.ibm.event.catalog.TableSchema;
import com.ibm.event.common.ConfigurationReader;
import com.ibm.event.oltp.EventContext;
import com.ibm.event.oltp.InsertResult;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.junit.*;


import static org.junit.Assert.assertEquals;


/**
 * This integration test can be run using a local installation of EventStore
 * The installer for EventStore can be obtained from:
 * https://www.ibm.com/us-en/marketplace/project-eventstore
 *
 * Note: Run each integration test (Java and Scala) one at the time
 *
 * Before running the test:
 * Change the host and port below in the function 'setEndpoint' to the EventStore
 * Change the host and port below in the function 'failureEndpoint' to a unresponsive host/port.
 *
 */
public class EventStoreSpec {
    private static ActorSystem system;
    private static Materializer materializer;
    private static EventContext eventContext;
    private static String databaseName = "TESTDB";
    private static String tableName = "TESTTABLE_JAVA";
    private static void setEndpoint() {
        // #configure-endpoint
        ConfigurationReader.setConnectionEndpoints("127.0.0.1:1100");
        ConfigurationReader.setConnectionTimeout(1);
        // #configure-endpoint

    }
    private static void setFailureEndpoint() {
        ConfigurationReader.setConnectionEndpoints("bogus:1100");
        ConfigurationReader.setConnectionTimeout(1);
    }

    private static Pair<ActorSystem, Materializer> setupMaterializer() {
        return Pair.create(system, materializer);
    }

    private static TableSchema getTableSchema() {
        Seq pkList = JavaConversions.asScalaBuffer(Collections.singletonList("id")).toSeq();
        List<StructField> fields = new ArrayList<>();

        fields.add(DataTypes.createStructField("id", DataTypes.IntegerType, false));
        fields.add(DataTypes.createStructField("someInt", DataTypes.IntegerType, false));
        fields.add(DataTypes.createStructField("someString", DataTypes.StringType, false));
        fields.add(DataTypes.createStructField("someBoolean", DataTypes.BooleanType, false));
        fields.add(DataTypes.createStructField("someOtherBoolean", DataTypes.BooleanType, true));

        return new TableSchema(tableName, DataTypes.createStructType(fields), pkList, pkList, scala.Option.apply(null));
    }

    @BeforeClass
    public static void setup() {
        system = ActorSystem.create();
        materializer = ActorMaterializer.create(system);
        setEndpoint();

        try {
            // Create new if no databases
            eventContext = EventContext.createDatabase(databaseName);
        } catch (Exception e) {
            // Use existing
            eventContext = EventContext.getEventContext(databaseName);
        }
    }

    @AfterClass
    public static void teardown() {
        setEndpoint();
        // #cleanup
        EventContext.cleanUp();
        // #cleanup
        JavaTestKit.shutdownActorSystem(system);
    }

    @Before
    public void createTable() {
        eventContext.createTable(getTableSchema());
    }

    @After
    public void dropTable() {
        eventContext.dropTable(tableName);
    }

    @Test
    public void testInsertingRecordsIntoTable() throws Exception {

        //#insert-rows
        List<Row> rows = new ArrayList<>();
        rows.add(RowFactory.create(1, 1, "Hello", true, false));
        rows.add(RowFactory.create(2, 1, "Hello", true, false));
        rows.add(RowFactory.create(3, 1, "Hello", true, false));

        Sink<Row, CompletionStage<Done>> sink = EventStoreSink.create(databaseName,tableName);
        final CompletionStage<Done> insertionResultFuture = Source.from(rows).runWith(sink, materializer);
        //#insert-rows

        insertionResultFuture.toCompletableFuture().get(5, TimeUnit.SECONDS);
    }

    @Test(expected = Exception.class)
    @Ignore
    public void testFailingtoInsertRecords() throws Exception {

        setFailureEndpoint();

        List<Row> rows = new ArrayList<>();
        rows.add(RowFactory.create(1, 1, "Hello", true, false));
        rows.add(RowFactory.create(2, 1, "Hello", true, false));
        rows.add(RowFactory.create(3, 1, "Hello", true, false));

        try {
            Sink<Row, CompletionStage<Done>> sink = EventStoreSink.create(databaseName, tableName);

            final CompletionStage<Done> insertionResultFuture = Source.from(rows).runWith(sink, materializer);
            insertionResultFuture.toCompletableFuture().get(5, TimeUnit.SECONDS);
        } finally {
            setEndpoint();
        }

    }

    @Test
    public void testInsertingRecordsIntoTableUsingFlow() throws Exception {

        //#insert-rows-using-flow
        List<Row> rows = new ArrayList<>();
        rows.add(RowFactory.create(1, 1, "Hello", true, false));
        rows.add(RowFactory.create(2, 1, "Hello", true, false));
        rows.add(RowFactory.create(3, 1, "Hello", true, false));

        Flow<Row, InsertResult, NotUsed> flow = EventStoreFlow.create(databaseName,tableName);
        final CompletionStage<List<InsertResult>> insertionResult = Source.from(rows).via(flow).runWith(Sink.seq(), materializer);
        //#insert-rows-using-flow

        final List<InsertResult> result = insertionResult.toCompletableFuture().get(5, TimeUnit.SECONDS);
        assertEquals(result.size(), 3);
    }

    @Test(expected = Exception.class)
    @Ignore
    public void testFailingtoInsertRecordsUsingFlow() throws Exception {

        setFailureEndpoint();

        List<Row> rows = new ArrayList<>();
        rows.add(RowFactory.create(1, 1, "Hello", true, false));
        rows.add(RowFactory.create(2, 1, "Hello", true, false));
        rows.add(RowFactory.create(3, 1, "Hello", true, false));

        try {
            Flow<Row, InsertResult, NotUsed> flow = EventStoreFlow.create(databaseName,tableName);
            final CompletionStage<List<InsertResult>> insertionResult = Source.from(rows).via(flow).runWith(Sink.seq(), materializer);
            insertionResult.toCompletableFuture().get(5, TimeUnit.SECONDS);
        } finally {
            // Make sure to reset the connection endpoint before exiting
            setEndpoint();
        }
    }

}
