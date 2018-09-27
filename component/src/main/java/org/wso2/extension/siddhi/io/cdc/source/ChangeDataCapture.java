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

import io.debezium.config.Configuration;
import io.debezium.embedded.EmbeddedEngine;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.log4j.Logger;
import org.wso2.extension.siddhi.io.cdc.util.Util;
import org.wso2.siddhi.core.exception.SiddhiAppRuntimeException;
import org.wso2.siddhi.core.stream.input.source.SourceEventListener;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Executor;

/**
 * This class is for capturing change data using debezium embedded engine.
 **/
class ChangeDataCapture implements Runnable {

    private final Logger logger = Logger.getLogger(ChangeDataCapture.class);
    private String operation;
    private Configuration config;
    private SourceEventListener sourceEventListener;
    private EmbeddedEngine engine;
    private CdcSource cdcSource;

    ChangeDataCapture(String operation, CdcSource cdcSource) {
        this.operation = operation;
        this.cdcSource = cdcSource;
    }

    void setSourceEventListener(SourceEventListener sourceEventListener) {
        this.sourceEventListener = sourceEventListener;
    }

    void setConfig(String username, String password, String url, String tableName
            , String historyFileDirectory, String siddhiAppName,
                   String siddhiStreamName, int serverID,
                   String serverName, String outServerName, String dbName, String pdbName, HashMap<String,
            String> connectorPropertiesMap)
            throws WrongConfigurationException {

        config = Configuration.empty();

        //extract details from the url.
        Map<String, String> urlDetails = Util.extractDetails(url);
        String schema = urlDetails.get("schema");
        String databaseName;
        String host = urlDetails.get("host");
        int port = Integer.parseInt(urlDetails.get("port"));
        String sid;


        //set schema specific connector properties
        switch (schema) {
            case "mysql":
                databaseName = urlDetails.get("database");
                config = config.edit().with("connector.class", "io.debezium.connector.mysql.MySqlConnector").build();
                if (port == -1) {
                    config = config.edit().with("database.port", 3306).build();
                } else {
                    config = config.edit().with("database.port", port).build();
                }

                //set the specified mysql table to be monitored
                config = config.edit().with("table.whitelist", databaseName + "." + tableName).build();
                break;
            case "oracle":
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
                //TODO: verify whitelist filter for oracle
                config = config.edit().with("table.whitelist", tableName).build();

                break;
            default:
                throw new WrongConfigurationException("Unsupported schema: " + schema);
        }

        //set hostname, username and the password for the connection
        config = config.edit().with("database.hostname", host)
                .with("database.user", username)
                .with("database.password", password).build();

        //set the serverID
        if (serverID == -1) {
            Random random = new Random();
            config = config.edit().with("server.id", random.ints(5400, 6400)).build();
        } else {
            config = config.edit().with("server.id", serverID).build();
        }

        //set the database server name if specified, otherwise set <host>_<port> as default
        if (serverName.equals("")) {
            config = config.edit().with("database.server.name", host + "_" + port).build();
        } else {
            config = config.edit().with("database.server.name", serverName).build();
        }

        //set the offset storage backing store class name and attach the cdcsource object.
        config = config.edit().with("offset.storage",
                InMemoryOffsetBackingStore.class.getName())
                .with("cdc.source.object", cdcSource)
                .build();

        //create the folders for history file if not exists
        File directory = new File(historyFileDirectory);
// TODO: 9/27/18 verify here
        if (!directory.exists()) {
            boolean isDirectoryCreated = directory.mkdirs();
            if (isDirectoryCreated) {
                logger.debug("Directory created for history file.");
            }
        }

        //set history file path.
        config = config.edit().with("database.history",
                "io.debezium.relational.history.FileDatabaseHistory")
                .with("database.history.file.filename",
                        historyFileDirectory + siddhiStreamName + ".dat").build();


        //set the offset.commit.policy to PeriodicSnapshotCommitOffsetPolicy.
        config = config.edit().with("offset.commit.policy",
                PeriodicSnapshotCommitOffsetPolicy.class.getName()).build();

        //set connector property: name
        config = config.edit().with("name", siddhiAppName + siddhiStreamName).build();

        //set additional connector properties using comma separated key value pair string
        for (Map.Entry<String, String> entry : connectorPropertiesMap.entrySet()) {
            config = config.edit().with(entry.getKey(), entry.getValue()).build();
        }
    }

    /**
     * Start the Debezium embedded engine with the configuration config and capture the change data.
     */
    private void captureChanges() {

        // Create the engine with above set configuration ...
        engine = EmbeddedEngine.create()
                .using(config)
                .notifying(this::handleEvent)
                .build();

        // Run the engine asynchronously ...
        Executor executor = new CDCExecutor();
        try {
            executor.execute(engine);
        } catch (Exception ex) {
            throw new SiddhiAppRuntimeException("Siddhi App run failed.");
        }
    }

    void stopEngine() {
        if (!engine.stop()) {
            logger.debug("Engine already stopped!");
        }
    }

    /**
     * When an event is received, create and send the event details to the sourceEventListener.
     */
    private void handleEvent(ConnectRecord connectRecord) {
        HashMap<String, Object> detailsMap = Util.createMap(connectRecord, operation);

        if (!detailsMap.isEmpty()) {
            sourceEventListener.onEvent(detailsMap, null);
        }
    }

    /**
     * When an object implementing interface <code>Runnable</code> is used
     * to create a thread, starting the thread causes the object's
     * <code>run</code> method to be called in that separately executing
     * thread.
     * <p>
     * The general contract of the method <code>run</code> is that it may
     * take any action whatsoever.
     *
     * @see Thread#run()
     */
    @Override
    public void run() {
        captureChanges();
    }

    /**
     * Executor class to execute the embedded engine and handle errors.
     */
    private static class CDCExecutor implements Executor {
        public void execute(Runnable command) {
            try {
                command.run();
            } catch (Exception ex) {
                final Logger logger = Logger.getLogger(CDCExecutor.class);
                logger.error("Error occured when running...");
                throw ex;
            }
        }
    }

    static class WrongConfigurationException extends Exception {
        WrongConfigurationException(String message) {
            super(message);
        }
    }
}
