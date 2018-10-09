/*
 * Copyright (c) 2018, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.extension.siddhi.io.cdc.source;

import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.exception.CannotRestoreSiddhiAppStateException;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.source.Source;
import org.wso2.siddhi.core.util.EventPrinter;
import org.wso2.siddhi.core.util.SiddhiTestHelper;
import org.wso2.siddhi.core.util.config.InMemoryConfigManager;
import org.wso2.siddhi.core.util.persistence.InMemoryPersistenceStore;
import org.wso2.siddhi.core.util.persistence.PersistenceStore;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class TestCaseOfCDCSource {

    private static final Logger logger = Logger.getLogger(TestCaseOfCDCSource.class);

    /**
     * Test case to extract details from URL with service name for Oracle
     */
    @Test
    public void urldetailExtractionOracle1() {

        String url1 = "jdbc:oracle:thin:@localhost:1521/oracle";

        //Map<String, String> details = CDCSourceUtil.extractDetails(url1);

        Map<String, String> expectedDetails = new HashMap<>();
        expectedDetails.put("host", "localhost");
        expectedDetails.put("schema", "oracle");
        expectedDetails.put("port", "1521");
        expectedDetails.put("service", "oracle");
        expectedDetails.put("driver", "thin");

        //assertEquals(expectedDetails, details);

    }

    /**
     * Test case to extract details from URL with SID for Oracle
     */
    @Test
    public void urldetailExtractionOracle2() {

        String url1 = "jdbc:oracle:thin:@localhost:1522:XE";

//        Map<String, String> details = CDCSourceUtil.extractDetails(url1);

        Map<String, String> expectedDetails = new HashMap<>();
        expectedDetails.put("schema", "oracle");
        expectedDetails.put("host", "localhost");
        expectedDetails.put("port", "1522");
        expectedDetails.put("sid", "XE");
        expectedDetails.put("driver", "thin");

//        assertEquals(expectedDetails, details);

    }

    /**
     * Test case to extract details from URL for MySQL.
     * host appear as an ip.
     */
    @Test
    public void urldetailExtractionMysql1() {

        String url1 = "jdbc:mysql://172.17.0.1:3306/testdb";

//        Map<String, String> details = CDCSourceUtil.extractDetails(url1);

        Map<String, String> expectedDetails = new HashMap<>();
        expectedDetails.put("schema", "mysql");
        expectedDetails.put("host", "172.17.0.1");
        expectedDetails.put("port", "3306");
        expectedDetails.put("database", "testdb");

//        assertEquals(expectedDetails, details);

    }

    /**
     * Test case to extract details from URL for Oracle
     * host appear as a name.
     */
    @Test
    public void urldetailExtractionMysql2() {

        String url1 = "jdbc:mysql://localhost:3306/testdb";

//        Map<String, String> details = CDCSourceUtil.extractDetails(url1);

        Map<String, String> expectedDetails = new HashMap<>();
        expectedDetails.put("schema", "mysql");
        expectedDetails.put("host", "localhost");
        expectedDetails.put("port", "3306");
        expectedDetails.put("database", "testdb");

//        assertEquals(expectedDetails, details);

    }

    /**
     * Test case to Capture Insert operations from a MySQL table.
     * Offset data persistence is enabled.
     */
    @Test
    public void cdcInsertOperationMysql() throws InterruptedException {
        logger.info("persistence test - cdc");

        PersistenceStore persistenceStore = new InMemoryPersistenceStore();

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setPersistenceStore(persistenceStore);

        String inStreamDefinition = "@app:name('cdcTesting')" +
                "@source(type = 'cdc'," +
                " url = 'jdbc:mysql://localhost:3306/SimpleDB'," +
                " username = 'debezium'," +
                " password = 'dbz'," +
                " table.name = 'login', " +
                " operation = 'insert', " +
                " @map(type='keyvalue'))" +
                "define stream istm (id string, name string);";

        String query = ("@info(name = 'query1') " +
                "from istm#log() " +
                "select *  " +
                "insert into outputStream;");


        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition +
                query);

        siddhiManager.setConfigManager(new InMemoryConfigManager());

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
//                EventPrinter.print(timeStamp, inEvents, removeEvents);
                for (Event event : inEvents) {
                    logger.info("received event: " + event);
                }
            }
        });

        siddhiAppRuntime.start();


        //persisting
        Thread.sleep(5000);
        siddhiAppRuntime.persist();

        Source source = null;
        Collection<List<Source>> collection = siddhiAppRuntime.getSources();
        for (List list : collection) {
            source = (Source) list.get(0);
            break;

        }
        source.pause();
        logger.info("persisted, paused...");
        Thread.sleep(10000);
        source.resume();
        logger.info("resumed..., running");


        //restarting siddhi app
        Thread.sleep(5000);
        logger.info("restarting...");
        siddhiAppRuntime.shutdown();
        // TODO: 10/4/18 don't create a new one? discuss.
        siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
//                EventPrinter.print(timeStamp, inEvents, removeEvents);
                for (Event event : inEvents) {
                    logger.info("received event from query 1: " + event);
                }
            }
        });
        siddhiAppRuntime.start();

        //loading
        try {
            siddhiAppRuntime.restoreLastRevision();
        } catch (CannotRestoreSiddhiAppStateException e) {
            Assert.fail("Restoring of Siddhi app " + siddhiAppRuntime.getName() + " failed", e);
        }

//        SiddhiTestHelper.waitForEvents(50, 100, new AtomicInteger(100), 1000000);
        while (true) {
            Thread.sleep(100);
        }
//        siddhiAppRuntime.shutdown();

        //TODO: 10/4/18 have test cases for all insert, up, del and persistence also
        //todo: use twi siddhi apps in automating
        // TODO: 10/4/18 check for validations
        // use fabric8
    }

    /**
     * Test case to Capture Delete operations from a MySQL table.
     */
    @Test
    public void cdcDeleteOperationMysql() throws InterruptedException {
        logger.info("------------------------------------------------------------------------------------------------");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "" +
                "@app:name('cdcTesting')" +
                "@source(type = 'cdc' , url = 'jdbc:mysql://localhost:3306/SimpleDB',  username = 'root'," +
                " password = '1234', table.name = 'login', " +
                " operation = 'delete'," +
                " @map(type='keyvalue'))" +
                "define stream istm (before_id string, before_name string);";
        String query = ("@info(name = 'query1') " +
                "from istm " +
                "select *  " +
                "insert into outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition +
                query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                for (Event event : inEvents) {
                    logger.info("received event: " + event);
                }
            }
        });

        siddhiAppRuntime.start();
        SiddhiTestHelper.waitForEvents(500, 10000000, new AtomicInteger(50), 10000);

        siddhiAppRuntime.shutdown();
    }

    /**
     * Test case to Capture insert operations from a MySQL table.
     */
    @Test
    public void cdcInsertOperationMysql2() throws InterruptedException {
        logger.info("------------------------------------------------------------------------------------------------");

        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "" +
                "@app:name('cdcTesting')" +
                "@source(type = 'cdc' , url = 'jdbc:mysql://localhost:3306/SimpleDB',  username = 'root'," +
                " password = '1234', table.name = 'people', " +
                " operation = 'insert'," +
                " @map(type='keyvalue'))" +
                "define stream istm (name string, age int);";
        String query = ("@info(name = 'query1') " +
                "from istm " +
                "select *  " +
                "insert into outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition +
                query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                for (Event event : inEvents) {
                    logger.info("received event: " + event);
                }
            }
        });

        siddhiAppRuntime.start();
        SiddhiTestHelper.waitForEvents(500, 999999999, new AtomicInteger(50), 10000);


        siddhiAppRuntime.shutdown();
    }

    /**
     * Test case to Capture Update operations from a MySQL table.
     */
    @Test
    public void cdcUpdateOperationMysql() throws InterruptedException {
        logger.info("------------------------------------------------------------------------------------------------");

        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "" +
                "@app:name('cdcTesting')" +
                "@source(type = 'cdc' , url = 'jdbc:mysql://localhost:3306/SimpleDB',  username = 'root'," +
                " password = '1234', table.name = 'login', " +
                " operation = 'update'," +
                " @map(type='keyvalue'))" +
                "define stream istm (id string, name string, before_id string, before_name string);";
        String query = ("@info(name = 'query1') " +
                "from istm " +
                "select *  " +
                "insert into outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition +
                query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                for (Event event : inEvents) {
                    logger.info("received event: " + event);
                }
            }
        });

        siddhiAppRuntime.start();
        SiddhiTestHelper.waitForEvents(500, 1000000, new AtomicInteger(50), 10000);

        siddhiAppRuntime.shutdown();
    }

    /**
     * Test case to Capture Insert operations from an Oracle table.
     */
    @Test
    public void cdcInsertOperationOracle() throws InterruptedException {
        logger.info("------------------------------------------------------------------------------------------------");

        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "" +
                "@app:name('cdcTesting')" +
                "@source(type = 'cdc' , url = 'jdbc:oracle:thin:@localhost:1521:XE', username = 'root'," +
                " password = '1234', table.name = 'login', database.out.server.name = 'xstrm'," +
                " operation = 'insert', " +
                " @map(type='keyvalue'))" +
                "define stream istm (id string, name string);";
        String query = ("@info(name = 'query1') " +
                "from istm " +
                "select *  " +
                "insert into outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition +
                query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                for (Event event : inEvents) {
                    logger.info("received event: " + event);
                }
            }
        });

        siddhiAppRuntime.start();
        SiddhiTestHelper.waitForEvents(50, 1, new AtomicInteger(50), 10000);

        siddhiAppRuntime.shutdown();
    }

}
