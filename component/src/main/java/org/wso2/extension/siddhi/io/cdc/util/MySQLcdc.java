package org.wso2.extension.siddhi.io.cdc.util;

import io.debezium.config.Configuration;
import io.debezium.embedded.EmbeddedEngine;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.log4j.Logger;

import java.net.URI;
import java.util.concurrent.Executor;

/**
 *
 * **/
public class MySQLcdc {

    private final Logger logger = Logger.getLogger(MySQLcdc.class);
    Configuration config;

    public boolean setConfig(String url, String offsetFileDirectory, String siddhiAppName, String siddhiStreamName, String commitPolicy,
                             int flushInterval, int serverID, String serverName, String outServerName, String dbName,
                             String pdbName) {

        Configuration config = Configuration.empty();

        //TODO:extract and validate the details from the url.
        //extract details from the url.
        String cleanURI = url.substring(5);
        URI uri = URI.create(cleanURI);
        String schema = uri.getScheme();
        String host = uri.getHost();
        int port = uri.getPort();
        String userName = "";
        String password = "";
        String SID = "";


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

        //set hostname, username and the password for the connection
        config = config.edit().with("database.hostname", host)
                .with("database.user", userName)
                .with("database.password", password).build();

        //set the serverID if specified
        if (serverID != -1) {
            config = config.edit().with("server.id", serverID).build();
        }

        //set offset backing storage to file backing storage
        config = config.edit().with("offset.storage",
                "org.apache.kafka.connect.storage.FileOffsetBackingStore").build();

        //set offset storage file name
        config = config.edit().with("offset.storage.file.filename",
                offsetFileDirectory + siddhiStreamName + ".dat").build();

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
        // Define the configuration for the embedded and MySQL connector ...
        Configuration config = Configuration.create()
                /* begin engine properties */
                .with("connector.class",
                        "io.debezium.connector.mysql.MySqlConnector")
                .with("offset.storage",
                        "org.apache.kafka.connect.storage.FileOffsetBackingStore")
                .with("offset.storage.file.filename",
                        "/home/chathuranga/mysqlLogs/offsetNew1.dat")
                .with("offset.flush.interval.ms", 60000)
                /* begin connector properties */
                .with("name", "my-sql-connector")
                .with("database.hostname", "localhost")
                .with("database.port", 3306)
                .with("database.user", "debezium")
                .with("database.password", "dbz")
                .with("server.id", 85743)
                .with("database.server.name", "my-sql-connector-server")
                .with("database.history",
                        "io.debezium.relational.history.FileDatabaseHistory")
                .with("database.history.file.filename",
                        "/home/chathuranga/mysqlLogs/dbhistoryNew1.dat")
                .build();

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
        logger.info(sourceRecord); //print the source record for now.
    }

    private static class MyExecutor implements Executor {
        public void execute(Runnable command) {
            command.run();
        }
    }
}
