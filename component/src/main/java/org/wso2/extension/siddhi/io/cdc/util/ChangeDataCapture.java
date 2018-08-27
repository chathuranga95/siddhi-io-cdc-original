package org.wso2.extension.siddhi.io.cdc.util;

import io.debezium.config.Configuration;
import io.debezium.embedded.EmbeddedEngine;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.log4j.Logger;
import org.wso2.siddhi.core.stream.input.source.SourceEventListener;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;

/**
 * Initiate and get MySQL database changes.
 **/
public class ChangeDataCapture {

    private final Logger logger = Logger.getLogger(ChangeDataCapture.class);
    Configuration config;
    SourceEventListener sourceEventListener;

    public void setSourceEventListener(SourceEventListener sourceEventListener) {
        this.sourceEventListener = sourceEventListener;
    }

    /*
     *Extract the details from the connection url and return as an array.
     * The element order:
     * 0: username
     * 1: password
     * 2: host
     * 3: port
     * 4: database name:
     * 5: SID
     * 6: driver
     * */
    private String[] extractDetails(String url) {
        String[] details = new String[7];

        

        return details;
    }

    public boolean setConfig(String url, String tableName, String offsetFileDirectory, String siddhiAppName,
                             String siddhiStreamName, String commitPolicy, int flushInterval, int serverID,
                             String serverName, String outServerName, String dbName, String pdbName) {

        this.config = Configuration.empty();

        //TODO:extract and validate the details from the url.
        //extract details from the url.

        String schema = "mysql";
        String databaseName = "SimpleDB";
        String host = "localhost";
        int port = -1;
        String userName = "root";
        String password = "1234";
        String sid = "";


        //set connector specific properties according to the schema.
        if (schema.equals("mysql")) {
            config = config.edit().with("connector.class", "io.debezium.connector.mysql.MySqlConnector").build();
            if (port == -1) {
                config = config.edit().with("database.port", 3306).build();
            } else {
                config = config.edit().with("database.port", port).build();
            }

        } else if (schema.equals("oracle")) {
            config = config.edit().with("connector.class", "io.debezium.connector.oracle.OracleConnector").build();
            if (port == -1) {
                config = config.edit().with("database.port", 1521).build();
            } else {
                config = config.edit().with("database.port", port).build();
            }
        } else {
            return false;
        }

        //set the specified table to be monitored
        config = config.edit().with("table.whitelist", databaseName + "." + tableName).build();

        //set hostname, username and the password for the connection
        config = config.edit().with("database.hostname", host)
                .with("database.user", userName)
                .with("database.password", password).build();

        //set the serverID anyway
        //if (serverID != -1) {
        config = config.edit().with("server.id", 5456).build();
        //}

        //set the database.server.name as host:port
        config = config.edit().with("database.server.name", "my-sql-connector-serve").build();

        //set offset backing storage to file backing storage
        config = config.edit().with("offset.storage",
                "org.apache.kafka.connect.storage.FileOffsetBackingStore").build();

        //set offset storage file name
        config = config.edit().with("offset.storage.file.filename",
                offsetFileDirectory + siddhiStreamName + ".dat").build();

        //set history file path details
        config = config.edit().with("database.history",
                "io.debezium.relational.history.FileDatabaseHistory")
                .with("database.history.file.filename",
                        "/home/chathuranga/mysqlLogs/dbhistoryNew1.dat").build();

        //set offset commit policy and the flush interval as necessary.
        if (commitPolicy.equals("PeriodicCommitOffsetPolicy")) {
            config = config.edit().with("offset.commit.policy",
                    "io.debezium.embedded.spi.OffsetCommitPolicy$PeriodicCommitOffsetPolicy")
                    .with("offset.flush.interval.ms", flushInterval).build();
        } else if (commitPolicy.equals("AlwaysCommitOffsetPolicy")) {
            //TODO:set the policy as Always commit and get the code to work.
            //periodic commit policy with a flush interval less than 0 behaves like always commit policy.
            config = config.edit().with("offset.commit.policy",
                    "io.debezium.embedded.spi.OffsetCommitPolicy$PeriodicCommitOffsetPolicy")
                    .with("offset.flush.interval.ms", -1).build();
        } else {
            return false;
        }

        //set connector properties
        config = config.edit().with("name", siddhiAppName + siddhiStreamName).build();

        return true;
    }

    public void captureChanges() {
        // Create the engine with this configuration ...
        EmbeddedEngine engine = EmbeddedEngine.create()
                .using(config)
                .notifying(this::handleEvent)
                .build();

        // Run the engine asynchronously ...
        Executor executor = new MyExecutor();
        executor.execute(engine);

    }


    private void handleEvent(SourceRecord sourceRecord) {
//        logger.info("Source record received from debezium: " + sourceRecord); //print the source record.

        Map<String, Object> hashMap = new HashMap<>();
        hashMap.put("operation", "This is the operation: INSERT");
        hashMap.put("raw_details", "This is raw details");
        sourceEventListener.onEvent(hashMap, null);
    }

    private static class MyExecutor implements Executor {
        public void execute(Runnable command) {
            command.run();
        }
    }
}
