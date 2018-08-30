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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.testng.AssertJUnit.assertEquals;

public class TestCaseOfCdcSource {
    // If you will know about this related testcase,
    //refer https://github.com/wso2-extensions/siddhi-io-file/blob/master/component/src/test
    private static final Logger LOG = Logger.getLogger(TestCaseOfCdcSource.class);

    @Test
    public void testTwitterStreaming2() {

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
    public void testTwitterStreaming3() {

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
    public void testTwitterStreaming4() {

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
    public void testTwitterStreaming1() throws InterruptedException {
        LOG.info("------------------------------------------------------------------------------------------------");

        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "" +
                "@app:name('cdcTesting')" +
                "@source(type = 'cdc' , url = 'jdbc:mysql://localhost:3306/SimpleDB', username = 'root'," +
                " password = '1234', table.name = 'login'," +
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
                    LOG.info("received event: " + event);
                }
            }
        });

        siddhiAppRuntime.start();
        SiddhiTestHelper.waitForEvents(50, 1, new AtomicInteger(50), 10000);

        siddhiAppRuntime.shutdown();
    }

}
