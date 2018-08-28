package org.wso2.extension.siddhi.io.cdc.util;

import io.debezium.config.Configuration;
import io.debezium.embedded.EmbeddedEngine;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.log4j.Logger;
import org.wso2.siddhi.core.stream.input.source.SourceEventListener;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Initiate and get MySQL database changes.
 **/
public class ChangeDataCapture {

    private final Logger logger = Logger.getLogger(ChangeDataCapture.class);
    Configuration config;
    SourceEventListener sourceEventListener;

    /*
     *Extract the details from the connection url and return as an array.
     *
     * mysql===> jdbc:mysql://hostname:port/testdb
     * oracle==> jdbc:oracle:thin:@hostname:port:SID
     *                  or
     *           jdbc:oracle:thin:@hostname:port/SERVICE
     * sqlserver => jdbc:sqlserver://hostname:port;databaseName=testdb
     * postgres ==> jdbc:postgresql://hostname:port/testdb
     *
     *
     * The elements in the hash-map order:
     * host
     * port
     * database name:
     * SID
     * driver
     * */
    public static Map<String, Object> extractDetails(String url) {
        Map<String, Object> details = new HashMap<>();
        String host;
        int port;
        String database;
        String driver;
        String sid;
        String service;

        String[] splittedURL = url.split(":");
        if (!splittedURL[0].equals("jdbc")) {
            throw new IllegalArgumentException("Invalid JDBC url.");
        } else {
            if (splittedURL[1].equals("mysql")) {
                //pattern match for mysql

                String regex = "jdbc:mysql://(\\w*|[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}):(\\d++)/(\\w*)";
                Pattern p = Pattern.compile(regex);
                Matcher matcher = p.matcher(url);
                if (matcher.find()) {
                    host = matcher.group(1);
                    port = Integer.valueOf(matcher.group(2));
                    database = matcher.group(3);

                } else {
                    // handle error appropriately
                    throw new IllegalArgumentException("Invalid JDBC url.");
                }


                details.put("database", database);

            } else if (splittedURL[1].equals("oracle")) {
                //pattern match for oracle
                //jdbc:oracle:thin:@hostname:port:SID


                String regex = "jdbc:oracle:(thin|oci):@(\\w*|[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}):" +
                        "(\\d++):(\\w*)";
                Pattern p = Pattern.compile(regex);

                Matcher matcher = p.matcher(url);
                if (matcher.find()) {
                    driver = matcher.group(1);
                    host = matcher.group(2);
                    port = Integer.valueOf(matcher.group(3));
                    sid = matcher.group(4);

                    details.put("sid", sid);

                } else {
                    //check for the service type url
                    String regexService = "jdbc:oracle:(thin|oci):" +
                            "@(\\w*|[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}):(\\d++)/(\\w*)";
                    Pattern patternService = Pattern.compile(regexService);

                    Matcher matcherService = patternService.matcher(url);
                    if (matcherService.find()) {
                        driver = matcherService.group(1);
                        host = matcherService.group(2);
                        port = Integer.valueOf(matcherService.group(3));
                        service = matcherService.group(4);

                    } else {
                        // handle error appropriately
                        throw new IllegalArgumentException("Invalid JDBC url for oracle service pattern.");
                    }
                    details.put("service", service);
                }

                details.put("driver", driver);

            } else {
                //for now checking for mysql and oracle
                throw new IllegalArgumentException("Invalid JDBC url.");
            }
            details.put("host", host);
            details.put("port", port);
        }
        return details;
    }

    public void setSourceEventListener(SourceEventListener sourceEventListener) {
        this.sourceEventListener = sourceEventListener;
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

        HashMap<String, Object> hashMap = new HashMap<>();
        hashMap.put("operation", "This is the operation: INSERT");
        hashMap.put("raw_details", "This is raw details");
        sourceEventListener.onEvent(hashMap, new String[1]);
    }

    private static class MyExecutor implements Executor {
        public void execute(Runnable command) {
            command.run();
        }
    }
}
