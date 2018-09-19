package org.wso2.extension.siddhi.io.cdc.source;

import org.apache.log4j.Logger;
import org.testng.annotations.Test;
import org.wso2.extension.siddhi.io.cdc.util.Util;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.util.EventPrinter;
import org.wso2.siddhi.core.util.SiddhiTestHelper;
import org.wso2.siddhi.core.util.config.InMemoryConfigManager;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.testng.AssertJUnit.assertEquals;

public class TestCaseOfCdcSource {
    // If you will know about this related testcase,
    //refer https://github.com/wso2-extensions/siddhi-io-file/blob/master/component/src/test
    private static final Logger logger = Logger.getLogger(TestCaseOfCdcSource.class);

    @Test
    public void urldetailExtractionOracle1() {

        String url1 = "jdbc:oracle:thin:@localhost:1521/oracle";

        Map<String, String> details = Util.extractDetails(url1);

        Map<String, String> expectedDetails = new HashMap<>();
        expectedDetails.put("host", "localhost");
        expectedDetails.put("schema", "oracle");
        expectedDetails.put("port", "1521");
        expectedDetails.put("service", "oracle");
        expectedDetails.put("driver", "thin");

        assertEquals(expectedDetails, details);

    }

    @Test
    public void urldetailExtractionOracle2() {

        String url1 = "jdbc:oracle:thin:@localhost:1522:XE";

        Map<String, String> details = Util.extractDetails(url1);

        Map<String, String> expectedDetails = new HashMap<>();
        expectedDetails.put("schema", "oracle");
        expectedDetails.put("host", "localhost");
        expectedDetails.put("port", "1522");
        expectedDetails.put("sid", "XE");
        expectedDetails.put("driver", "thin");

        assertEquals(expectedDetails, details);

    }

    @Test
    public void urldetailExtractionMysql1() {

        String url1 = "jdbc:mysql://172.17.0.1:3306/testdb";

        Map<String, String> details = Util.extractDetails(url1);

        Map<String, String> expectedDetails = new HashMap<>();
        expectedDetails.put("schema", "mysql");
        expectedDetails.put("host", "172.17.0.1");
        expectedDetails.put("port", "3306");
        expectedDetails.put("database", "testdb");

        assertEquals(expectedDetails, details);

    }

    @Test
    public void urldetailExtractionMysql2() {

        String url1 = "jdbc:mysql://localhost:3306/testdb";

        Map<String, String> details = Util.extractDetails(url1);

        Map<String, String> expectedDetails = new HashMap<>();
        expectedDetails.put("schema", "mysql");
        expectedDetails.put("host", "localhost");
        expectedDetails.put("port", "3306");
        expectedDetails.put("database", "testdb");

        assertEquals(expectedDetails, details);

    }

    @Test
    public void cdcInsertOperationMysql() throws InterruptedException {
        logger.info("------------------------------------------------------------------------------------------------");

        SiddhiManager siddhiManager = new SiddhiManager();

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
                "from istm " +
                "select *  " +
                "insert into outputStream;");



        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition +
                query);

        siddhiManager.setConfigManager(new InMemoryConfigManager());

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                for (Event event : inEvents) {
                    logger.info("received event from query 1: " + event);
                }
            }
        });

        siddhiAppRuntime.start();
        SiddhiTestHelper.waitForEvents(50, 1, new AtomicInteger(1), 10000);
        siddhiAppRuntime.shutdown();


    }


    @Test
    public void cdcDeleteOperationMysql() throws InterruptedException {
        logger.info("------------------------------------------------------------------------------------------------");

        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "" +
                "@app:name('cdcTesting')" +
                "@source(type = 'cdc' , url = 'jdbc:mysql://localhost:3306/SimpleDB',  username = 'root'," +
                " password = '1234', table.name = 'login', " +
                " offsets.commit.policy = 'AlwaysCommitOffsetPolicy'," +
                " operation = 'delete', database.server.id = '1300'," +
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
        SiddhiTestHelper.waitForEvents(50, 1, new AtomicInteger(50), 10000);

        siddhiAppRuntime.shutdown();
    }

    @Test
    public void cdcUpdateOperationMysql() throws InterruptedException {
        logger.info("------------------------------------------------------------------------------------------------");

        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "" +
                "@app:name('cdcTesting')" +
                "@source(type = 'cdc' , url = 'jdbc:mysql://localhost:3306/SimpleDB',  username = 'root'," +
                " password = '1234', table.name = 'login', " +
                " offsets.commit.policy = 'AlwaysCommitOffsetPolicy'," +
                " operation = 'update', database.server.id = '1300'," +
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
        SiddhiTestHelper.waitForEvents(50, 1, new AtomicInteger(50), 10000);

        siddhiAppRuntime.shutdown();
    }

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
