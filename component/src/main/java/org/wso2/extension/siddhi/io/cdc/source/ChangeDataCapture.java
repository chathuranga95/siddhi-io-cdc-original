package org.wso2.extension.siddhi.io.cdc.source;

import io.debezium.config.Configuration;
import io.debezium.embedded.EmbeddedEngine;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.log4j.Logger;
import org.wso2.extension.siddhi.io.cdc.util.Util;
import org.wso2.siddhi.core.stream.input.source.SourceEventListener;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Executor;

/**
 * Initiate and get MySQL database changes.
 **/
public class ChangeDataCapture {

    private final Logger logger = Logger.getLogger(ChangeDataCapture.class);
    Configuration config;
    String operation;
    private SourceEventListener sourceEventListener;

    public void setSourceEventListener(SourceEventListener sourceEventListener) {
        this.sourceEventListener = sourceEventListener;
    }

    public boolean setConfig(String username, String password, String url, String tableName,
                             String offsetFileDirectory, String siddhiAppName,
                             String siddhiStreamName, String commitPolicy, int flushInterval, int serverID,
                             String serverName, String outServerName, String dbName, String pdbName) {

        this.config = Configuration.empty();

        //extract details from the url.

        Map<String, String> urlDetails = Util.extractDetails(url);

        String schema = urlDetails.get("schema");
        String databaseName;
        String host = urlDetails.get("host");
        int port = Integer.parseInt(urlDetails.get("port"));
        String sid;


        //set connector specific properties according to the schema.
        if (schema.equals("mysql")) {
            databaseName = urlDetails.get("database");
            config = config.edit().with("connector.class", "io.debezium.connector.mysql.MySqlConnector").build();
            if (port == -1) {
                config = config.edit().with("database.port", 3306).build();
            } else {
                config = config.edit().with("database.port", port).build();
            }

            //set the specified mysql table to be monitored
            config = config.edit().with("table.whitelist", databaseName + "." + tableName).build();


        } else if (schema.equals("oracle")) {
            sid = urlDetails.get("sid");
            config = config.edit().with("connector.class", "io.debezium.connector.oracle.OracleConnector").build();
            if (port == -1) {
                config = config.edit().with("database.port", 1521).build();
            } else {
                config = config.edit().with("database.port", port).build();
            }

            //set the dbname if specified, otherwise set sid as default
            if (dbName.equals("")) {
                config = config.edit().with("database.dbname", sid).build();
            } else {
                config = config.edit().with("database.dbname", dbName).build();
            }

            //set the pdb name if specified
            if (pdbName.equals("")) {
                config = config.edit().with("database.pdb.name", pdbName).build();
            }

            //set the xstream outbound server
            config = config.edit().with("database.out.server.name", outServerName).build();


            //set the specified oracle table to be monitored
            //TODO: verify whitelist filter
            config = config.edit().with("table.whitelist", tableName).build();

        } else {
            return false;
        }

        //set hostname, username and the password for the connection
        config = config.edit().with("database.hostname", host)
                .with("database.user", username)
                .with("database.password", password).build();

        //set the serverID
        if (serverID != -1) {
            Random random = new Random();
            config = config.edit().with("server.id", random.ints(5400, 6400)).build();
        } else {
            config = config.edit().with("server.id", serverID).build();
        }

        //set the database server name if specified, otherwise set <host>:<port> as default
        if (serverName.equals("")) {
            config = config.edit().with("database.server.name", host + ":" + port).build();
        } else {
            config = config.edit().with("database.server.name", serverName).build();
        }

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

        //TODO: Implement and set the offset commit policy with a custom in-memory policy.
        //TODO:Sync the offset committing with the SP's periodic snapshot
        //set offset commit policy and the flush interval as necessary.
        if (commitPolicy.equals("PeriodicCommitOffsetPolicy")) {
            config = config.edit().with("offset.commit.policy",
                    "io.debezium.embedded.spi.OffsetCommitPolicy$PeriodicCommitOffsetPolicy")
                    .with("offset.flush.interval.ms", flushInterval).build();
        } else if (commitPolicy.equals("AlwaysCommitOffsetPolicy")) {
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


    public void captureChanges(String operation) {
        this.operation = operation;

        // Create the engine with this configuration ...
        EmbeddedEngine engine = EmbeddedEngine.create()
                .using(config)
                .notifying(this::handleEvent)
                .build();

        // Run the engine asynchronously ...
        Executor executor = new MyExecutor();
        executor.execute(engine);

    }

    public void handleEvent(ConnectRecord connectRecord) {
        logger.info("Connect record received from debezium: " + connectRecord);

        HashMap<String, String> detailsMap = Util.createMap(connectRecord, operation);

        if (!detailsMap.isEmpty()) {
            sourceEventListener.onEvent(detailsMap, null);
            logger.info("Map sent: " + detailsMap);
        }


    }

    private static class MyExecutor implements Executor {
        public void execute(Runnable command) {
            command.run();
        }
    }
}
