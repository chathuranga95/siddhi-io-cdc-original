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
import org.wso2.extension.siddhi.io.cdc.util.CDCSourceConstants;
import org.wso2.extension.siddhi.io.cdc.util.CDCSourceUtil;
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
                   String serverName, HashMap<String,
            String> connectorPropertiesMap)
            throws WrongConfigurationException {

        config = Configuration.empty();

        //extract details from the url.
        Map<String, String> urlDetails = CDCSourceUtil.extractDetails(url);
        String schema = urlDetails.get("schema");
        String databaseName;
        String host = urlDetails.get("host");
        int port = Integer.parseInt(urlDetails.get("port"));


        //set schema specific connector properties
        switch (schema) {
            case "mysql":
                databaseName = urlDetails.get("database");
                config = config.edit().with(CDCSourceConstants.CONNECTOR_CLASS,
                        "io.debezium.connector.mysql.MySqlConnector").build();
                if (port == -1) {
                    config = config.edit().with(CDCSourceConstants.DATABASE_PORT, 3306).build();
                } else {
                    config = config.edit().with(CDCSourceConstants.DATABASE_PORT, port).build();
                }

                //set the specified mysql table to be monitored
                config = config.edit().with(CDCSourceConstants.TABLE_WHITELIST, databaseName + "." + tableName).build();
                break;

            default:
                throw new WrongConfigurationException("Unsupported schema: " + schema);
        }

        //set hostname, username and the password for the connection
        config = config.edit().with(CDCSourceConstants.DATABASE_HOSTNAME, host)
                .with(CDCSourceConstants.DATABASE_USER, username)
                .with(CDCSourceConstants.DATABASE_PASSWORD, password).build();

        //set the serverID
        if (serverID == -1) {
            Random random = new Random();
            config = config.edit().with(CDCSourceConstants.SERVER_ID, random.ints(5400, 6400)).build();
        } else {
            config = config.edit().with(CDCSourceConstants.SERVER_ID, serverID).build();
        }

        //set the database server name if specified, otherwise set <host>_<port> as default
        if (serverName.equals("")) {
            config = config.edit().with(CDCSourceConstants.DATABASE_SERVER_NAME, host + "_" + port).build();
        } else {
            config = config.edit().with(CDCSourceConstants.DATABASE_SERVER_NAME, serverName).build();
        }

        //set the offset storage backing store class name and attach the cdcsource object.
        config = config.edit().with(CDCSourceConstants.OFFSET_STORAGE,
                InMemoryOffsetBackingStore.class.getName())
                .with(CDCSourceConstants.CDC_SOURCE_OBJECT, cdcSource)
                .build();

        //create the folders for history file if not exists
        File directory = new File(historyFileDirectory);
        if (!directory.exists()) {
            boolean isDirectoryCreated = directory.mkdirs();
            if (isDirectoryCreated) {
                logger.debug("Directory created for history file.");
            }
        }

        //set history file path.
        config = config.edit().with(CDCSourceConstants.DATABASE_HISTORY,
                "io.debezium.relational.history.FileDatabaseHistory")
                .with(CDCSourceConstants.DATABASE_HISTORY_FILE_NAME,
                        historyFileDirectory + siddhiStreamName + ".dat").build();


        //set the offset.commit.policy to PeriodicSnapshotCommitOffsetPolicy.
        config = config.edit().with(CDCSourceConstants.OFFSET_COMMIT_POLICY,
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

    /**
     * When an event is received, create and send the event details to the sourceEventListener.
     */
    private void handleEvent(ConnectRecord connectRecord) {
        HashMap<String, Object> detailsMap = CDCSourceUtil.createMap(connectRecord, operation);

        if (!detailsMap.isEmpty()) {
            sourceEventListener.onEvent(detailsMap, null);
        }
    }

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
