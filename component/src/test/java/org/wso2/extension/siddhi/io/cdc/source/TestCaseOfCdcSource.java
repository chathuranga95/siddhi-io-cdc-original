package org.wso2.extension.siddhi.io.cdc.source;

import org.apache.log4j.Logger;
import org.testng.annotations.Test;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.util.EventPrinter;
import org.wso2.siddhi.core.util.SiddhiTestHelper;

import java.util.concurrent.atomic.AtomicInteger;

public class TestCaseOfCdcSource {
    // If you will know about this related testcase,
    //refer https://github.com/wso2-extensions/siddhi-io-file/blob/master/component/src/test
    private static final Logger LOG = Logger.getLogger(TestCaseOfCdcSource.class);

    @Test
    public void testTwitterStreaming1() throws InterruptedException {
        LOG.info("------------------------------------------------------------------------------------------------");

        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "" +
                "@app:name('cdcTesting')" +
                "@source(type = 'cdc' , url = 'option_value', table.name = 'login', operation = 'insert'," +
                " @map(type='keyvalue', fail.on.missing.attribute = 'false'))" +
                "define stream istm (operation String, raw_details String);";
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
