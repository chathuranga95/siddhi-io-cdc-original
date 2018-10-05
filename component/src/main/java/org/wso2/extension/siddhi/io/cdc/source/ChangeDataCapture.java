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
import org.wso2.siddhi.core.stream.input.source.SourceEventListener;

import java.io.File;
import java.util.Map;
import java.util.Random;

/**
 * This class is for capturing change data using debezium embedded engine.
 **/
class ChangeDataCapture {

    private static final Logger log = Logger.getLogger(ChangeDataCapture.class);
    private String operation;
    private Configuration config;
    private SourceEventListener sourceEventListener;
    private int cdcSourceHashCode;

    ChangeDataCapture(String operation, int cdcSourceHashCode) {
        this.operation = operation;
        this.cdcSourceHashCode = cdcSourceHashCode;
    }

    /**
     * @param sourceEventListener is used to initialize this.sourceEventListener.
     */
    void setSourceEventListener(SourceEventListener sourceEventListener) {
        this.sourceEventListener = sourceEventListener;
    }

    /**
     * Initialize this.config according to user specified parameters.
     */
    void setConfig(String username, String password, String url, String tableName
            , String historyFileDirectory, String siddhiAppName,
                   String siddhiStreamName, int serverID,
                   String serverName, Map<String,
            String> connectorPropertiesMap)
            throws WrongConfigurationException {

        config = Configuration.empty();

        //extract details from the url.
        Map<String, String> urlDetails = CDCSourceUtil.extractDetails(url);
        String schema = urlDetails.get("schema");
        String databaseName;
        String host = urlDetails.get("host");
        int port = Integer.parseInt(urlDetails.get("port"));


        // TODO: 10/4/18 let the user to set the connector class, discuss with Tishan ayiya
        //set schema specific connector properties
        switch (schema) {
            case "mysql":
                databaseName = urlDetails.get("database");
                config = config.edit().with(CDCSourceConstants.CONNECTOR_CLASS,
                        CDCSourceConstants.MYSQL_CONNECTOR_CLASS).build();
                if (port == -1) {
                    config = config.edit().with(CDCSourceConstants.DATABASE_PORT, 3306).build();
                } else {
                    config = config.edit().with(CDCSourceConstants.DATABASE_PORT, port).build();
                }

                //set the specified mysql table to be monitored
                config = config.edit().with(CDCSourceConstants.TABLE_WHITELIST, databaseName + "." + tableName).build();
                break;
            default:
                throw new WrongConfigurationException("Unsupported schema. Expected schema: mysql, Found: " + schema);
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
                .with(CDCSourceConstants.CDC_SOURCE_OBJECT, cdcSourceHashCode)
                .build();

        //create the folders for history file if not exists
        File directory = new File(historyFileDirectory);
        if (!directory.exists()) {
            boolean isDirectoryCreated = directory.mkdirs();
            if (isDirectoryCreated) {
                log.debug("Directory created for history file.");
            }
        }

        //set history file path.
        config = config.edit().with(CDCSourceConstants.DATABASE_HISTORY,
                CDCSourceConstants.DATABASE_HISTORY_FILEBASE_HISTORY)
                .with(CDCSourceConstants.DATABASE_HISTORY_FILE_NAME,
                        historyFileDirectory + siddhiStreamName + ".dat").build();


        //set the offset.commit.policy to PeriodicSnapshotCommitOffsetPolicy.
        config = config.edit().with(CDCSourceConstants.OFFSET_COMMIT_POLICY,
                MyCommitPolicy.class.getName()).build();

        //set connector property: name
        config = config.edit().with("name", siddhiAppName + siddhiStreamName).build();

        //set additional connector properties using comma separated key value pair string
        for (Map.Entry<String, String> entry : connectorPropertiesMap.entrySet()) {
            config = config.edit().with(entry.getKey(), entry.getValue()).build();
        }
    }

    /**
     * Create a new Debezium embedded engine with the configuration {@code config} and,
     *
     * @return engine.
     */
    EmbeddedEngine getEngine() {
        // Create an engine with above set configuration ...
        EmbeddedEngine engine = EmbeddedEngine.create()
                .using(config)
                .notifying(this::handleEvent)
                .build();

        return engine;
    }

    /**
     * When an event is received, create and send the event details to the sourceEventListener.
     */
    private void handleEvent(ConnectRecord connectRecord) {
        Map<String, Object> detailsMap = CDCSourceUtil.createMap(connectRecord, operation);

        if (!detailsMap.isEmpty()) {
            sourceEventListener.onEvent(detailsMap, null);
        }
    }
}
