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

import org.wso2.extension.siddhi.io.cdc.util.CDCSourceConstants;
import org.wso2.extension.siddhi.io.cdc.util.Util;
import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.annotation.Parameter;
import org.wso2.siddhi.annotation.SystemParameter;
import org.wso2.siddhi.annotation.util.DataType;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.exception.SiddhiAppCreationException;
import org.wso2.siddhi.core.stream.input.source.Source;
import org.wso2.siddhi.core.stream.input.source.SourceEventListener;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.core.util.transport.OptionHolder;
import org.wso2.siddhi.query.api.exception.SiddhiAppValidationException;

import java.util.HashMap;
import java.util.Map;


/**
 * Extension to the WSO2 Stream Processor to retrieve Database Changes - implementation of cdc source.
 **/
@Extension(
        name = "cdc",
        namespace = "source",
        description = "The cdc source receives events when the MySQL database's change event " +
                "(INSERT, UPDATE, DELETE) is triggered. The events are received in key-value map format." +
                "The following are key values of the map of a cdc change event and their descriptions.\n" +
                "X : The table's column X value after the event occurred. Applicable when 'insert' or 'update'" +
                " operations are specified. \n" +
                "before_X : The table's column X value before the event occurred. Applicable when 'delete' or " +
                "'update' operations are specified.\n",
        parameters = {
                @Parameter(name = "url",
                        description = "Connection url to the database." +
                                "use format:" +
                                "for mysql--> jdbc:mysql://<host>:<port>/<database_name> " +
                                "for oracle--> jdbc:oracle:<driver>:@<host>:<port>:<SID>",
                        type = DataType.STRING
                ),
                @Parameter(
                        name = "username",
                        description = "Username of the user created in the prerequisites",
                        type = DataType.STRING
                ),
                @Parameter(
                        name = "password",
                        description = "Password for the user",
                        type = DataType.STRING
                ),
                @Parameter(
                        name = "table.name",
                        description = "Name of the table which needs to be monitored for data changes",
                        type = DataType.STRING
                ),
                @Parameter(
                        name = "operation",
                        description = "interested change event name. 'insert', 'update' or 'delete'",
                        type = DataType.STRING
                ),
                @Parameter(
                        name = "connector.properties",
                        description = "Debezium connector specified properties as a comma separated string. " +
                                "Previously set values will be overridden by this properties.",
                        type = DataType.STRING,
                        optional = true,
                        defaultValue = "<Empty_String>"
                )
        },
        systemParameter = {
                @SystemParameter(name = "offsets.commit.policy",
                        description = "The name of the Java class of the commit policy. When this policy triggers," +
                                " data offsets will be flushed. Should be one of 'PeriodicCommitOffsetPolicy' or " +
                                "AlwaysCommitOffsetPolicy",
                        defaultValue = "PeriodicCommitOffsetPolicy",
                        possibleParameters = {"PeriodicCommitOffsetPolicy", "AlwaysCommitOffsetPolicy"}
                ),
                @SystemParameter(name = "offsets.file.directory",
                        description = "Path to store the file with the connector’s change data offsets.",
                        defaultValue = "{WSO2SP_HOME}/cdc/offset/{SiddhiAppName}",
                        possibleParameters = {"<Any user Read/Writable directory>"}
                ),
                @SystemParameter(name = "offsets.flush.intervalms",
                        description = "Time in milliseconds to flush offsets when the commit policy is set to File",
                        defaultValue = "60000",
                        possibleParameters = {"<Any non negative integer>"}
                ),
                @SystemParameter(name = "database.history.file.directory",
                        description = "Path to store database schema history changes.",
                        defaultValue = "{WSO2SP_HOME}/cdc/history/{SiddhiAppName}",
                        possibleParameters = {"<Any user Read/Writable directory>"}
                ),
                @SystemParameter(name = "database.server.name",
                        description = "Logical name that identifies and provides a namespace for the " +
                                "particular database server",
                        defaultValue = "{host}_{port}",
                        possibleParameters = {"<Unique name to connect to the database cluster>"}
                ),
                @SystemParameter(name = "database.server.id",
                        description = "For MySQL, a unique integer between 1 to 2^32 as the ID," +
                                " This is used when joining MySQL database cluster to read binlog",
                        defaultValue = "<random integer between 5400 and 6400>",
                        possibleParameters = {"<Unique server id to connect to the database cluster>"}
                ),
                @SystemParameter(name = "database.out.server.name",
                        description = "Oracle Xstream outbound server name for Oracle. Required for Oracle database",
                        defaultValue = "<not applicable>",
                        possibleParameters = {"<oracle's outbound server name>"}
                ),
                @SystemParameter(name = "database.dbname",
                        description = "Name of the database to connect to. Must be the CDB name" +
                                " when working with the CDB + PDB model.",
                        defaultValue = "{sid}",
                        possibleParameters = {"<SID>"}
                ),
                @SystemParameter(name = "database.pdb.name",
                        description = "Name of the PDB to connect to. Required when working with the CDB + PDB model.",
                        defaultValue = "<not applicable>",
                        possibleParameters = {"<Pluggable database name>"}
                )
        },
        examples = {
                @Example(
                        syntax = "@source(type = 'cdc' , url = 'jdbc:mysql://localhost:3306/SimpleDB'," +
                                " username = 'cdcuser', password = 'pswd4cdc', table.name = 'students'," +
                                " operation = 'insert', @map(type='keyvalue', @attributes(id = 'id', name = 'name')))" +
                                "define stream inputStream (id string, name string);",
                        description = "In this example, the cdc source starts listening to the row insertions" +
                                " on students table which is under MySQL SimpleDB database that" +
                                " can be accessed with the given url"
                ),
                @Example(
                        syntax = "@source(type = 'cdc' , url = 'jdbc:mysql://localhost:3306/SimpleDB'," +
                                " username = 'cdcuser', password = 'pswd4cdc', table.name = 'students'," +
                                " operation = 'update', @map(type='keyvalue', @attributes(id = 'id', name = 'name', " +
                                "before_id = 'before_id', before_name = 'before_name')))" +
                                "define stream inputStream (id string, name string, before_id string," +
                                " before_name string);",
                        description = "In this example, the cdc source starts listening to the row updates" +
                                " on students table which is under MySQL SimpleDB database that" +
                                " can be accessed with the given url"
                ),
                @Example(
                        syntax = "@source(type = 'cdc' , url = 'jdbc:mysql://localhost:3306/SimpleDB'," +
                                " username = 'cdcuser', password = 'pswd4cdc', table.name = 'students'," +
                                " operation = 'delete', @map(type='keyvalue', @attributes(before_id = 'before_id'," +
                                " before_name = 'before_name')))" +
                                "define stream inputStream (before_id string, before_name string);",
                        description = "In this example, the cdc source starts listening to the row deletions" +
                                " on students table which is under MySQL SimpleDB database that" +
                                " can be accessed with the given url"
                )
        }
)

// for more information refer https://wso2.github.io/siddhi/documentation/siddhi-4.0/#sources
public class CdcSource extends Source {

    private HashMap<byte[], byte[]> cache = new HashMap<>();
    private HashMap<String, String> connectorPropertiesMap = new HashMap<>();
    private String outboundServerName;
    private String url;
    private String operation;
    private String offsetFileDirectory;
    private String commitPolicy;
    private ChangeDataCapture changeDataCapture;
    private String historyFileDirectory;
    private String connectorProperties;

    /**
     * The initialization method for {@link Source}, will be called before other methods. It used to validate
     * all configurations and to get initial values.
     *
     * @param sourceEventListener After receiving events, the source should trigger onEvent() of this listener.
     *                            Listener will then pass on the events to the appropriate mappers for processing .
     * @param optionHolder        Option holder containing static configuration related to the {@link Source}
     * @param configReader        ConfigReader is used to read the {@link Source} related system configuration.
     * @param siddhiAppContext    the context of the {@link org.wso2.siddhi.query.api.SiddhiApp} used to get Siddhi
     *                            related utility functions.
     */


    @Override
    public void init(SourceEventListener sourceEventListener, OptionHolder optionHolder,
                     String[] requestedTransportPropertyNames, ConfigReader configReader,
                     SiddhiAppContext siddhiAppContext) {


        String siddhiAppName = siddhiAppContext.getName();
        String streamName = sourceEventListener.getStreamDefinition().getId();

        //initialize mandatory parameters
        url = optionHolder.validateAndGetOption(CDCSourceConstants.DATABASE_CONNECTION_URL).getValue();
        String tableName = optionHolder.validateAndGetOption(CDCSourceConstants.TABLE_NAME).getValue();
        String username = optionHolder.validateAndGetOption(CDCSourceConstants.USERNAME).getValue();
        String password = optionHolder.validateAndGetOption(CDCSourceConstants.PASSWORD).getValue();
        operation = optionHolder.validateAndGetOption(CDCSourceConstants.OPERATION).getValue();

        //initialize system parameters from annotations, deployment config file or default values

        if (optionHolder.isOptionExists(CDCSourceConstants.OFFSET_FILE_DIRECTORY)) {
            offsetFileDirectory = optionHolder.validateAndGetOption(CDCSourceConstants.OFFSET_FILE_DIRECTORY)
                    .getValue();
        } else {
            offsetFileDirectory = configReader.readConfig(CDCSourceConstants.OFFSET_FILE_DIRECTORY,
                    Util.getStreamProcessorPath() + "cdc/offsets/" + siddhiAppName + "/");
        }

        if (optionHolder.isOptionExists(CDCSourceConstants.DATABASE_HISTORY_FILE_DIRECTORY)) {
            historyFileDirectory = optionHolder.validateAndGetOption(
                    CDCSourceConstants.DATABASE_HISTORY_FILE_DIRECTORY).getValue();
        } else {
            historyFileDirectory = configReader.readConfig(
                    CDCSourceConstants.DATABASE_HISTORY_FILE_DIRECTORY,
                    Util.getStreamProcessorPath() + "cdc/history/" + siddhiAppName + "/");
        }

        if (optionHolder.isOptionExists(CDCSourceConstants.OFFSET_COMMIT_POLICY)) {
            commitPolicy = optionHolder.validateAndGetOption(CDCSourceConstants.OFFSET_COMMIT_POLICY).getValue();
        } else {
            commitPolicy = configReader.readConfig(CDCSourceConstants.OFFSET_COMMIT_POLICY,
                    CDCSourceConstants.PERIODIC_OFFSET_COMMIT_POLICY);
        }

        int flushInterval;
        if (optionHolder.isOptionExists(CDCSourceConstants.OFFSET_FLUSH_INTERVALMS)) {
            flushInterval = Integer.parseInt(optionHolder.validateAndGetOption(
                    CDCSourceConstants.OFFSET_FLUSH_INTERVALMS).getValue());
        } else {
            flushInterval = Integer.parseInt(configReader.readConfig(CDCSourceConstants.OFFSET_FLUSH_INTERVALMS,
                    "60000"));
        }

        int serverID;
        if (optionHolder.isOptionExists(CDCSourceConstants.DATABASE_SERVER_ID)) {
            serverID = Integer.parseInt(optionHolder.validateAndGetOption(CDCSourceConstants.DATABASE_SERVER_ID)
                    .getValue());
        } else {
            serverID = Integer.parseInt(configReader.readConfig(CDCSourceConstants.DATABASE_SERVER_ID, "-1"));
        }

        String serverName;
        if (optionHolder.isOptionExists(CDCSourceConstants.DATABASE_SERVER_NAME)) {
            serverName = optionHolder.validateAndGetOption(CDCSourceConstants.DATABASE_SERVER_NAME).getValue();
        } else {
            serverName = configReader.readConfig(CDCSourceConstants.DATABASE_SERVER_NAME,
                    CDCSourceConstants.EMPTY_STRING);
        }

        if (optionHolder.isOptionExists(CDCSourceConstants.DATABASE_OUT_SERVER_NAME)) {
            outboundServerName = optionHolder.validateAndGetOption(CDCSourceConstants.DATABASE_OUT_SERVER_NAME)
                    .getValue();
        } else {
            outboundServerName = configReader.readConfig(CDCSourceConstants.DATABASE_OUT_SERVER_NAME,
                    CDCSourceConstants.EMPTY_STRING);
        }

        String dbName;
        if (optionHolder.isOptionExists(CDCSourceConstants.DATABASE_DBNAME)) {
            dbName = optionHolder.validateAndGetOption(CDCSourceConstants.DATABASE_DBNAME).getValue();
        } else {
            dbName = configReader.readConfig(CDCSourceConstants.DATABASE_DBNAME,
                    CDCSourceConstants.EMPTY_STRING);
        }

        String pdbName;
        if (optionHolder.isOptionExists(CDCSourceConstants.DATABASE_PDB_NAME)) {
            pdbName = optionHolder.validateAndGetOption(CDCSourceConstants.DATABASE_PDB_NAME).getValue();
        } else {
            pdbName = configReader.readConfig(CDCSourceConstants.DATABASE_PDB_NAME, CDCSourceConstants.EMPTY_STRING);
        }

        //initialize parameters from connector.properties
        connectorProperties = optionHolder.validateAndGetStaticValue(CDCSourceConstants.CONNECTOR_PROPERTIES,
                CDCSourceConstants.EMPTY_STRING);

        validateParameter();


        this.changeDataCapture = new ChangeDataCapture();

        //send this object reference to changeDataCapture object
        changeDataCapture.setCdcSource(this);

        //keep the object reference in Object keeper
        ObjectKeeper.addCdcObject(this);

        try {
            changeDataCapture.setConfig(username, password, url, tableName, offsetFileDirectory, historyFileDirectory,
                    siddhiAppName, streamName, commitPolicy, flushInterval, serverID, serverName,
                    outboundServerName, dbName, pdbName, connectorPropertiesMap);
            changeDataCapture.setSourceEventListener(sourceEventListener);
        } catch (ChangeDataCapture.WrongConfigurationException ex) {
            throw new SiddhiAppCreationException("The cdc source couldn't get started. Invalid" +
                    " configuration parameters.");
        }
    }

    /**
     * Returns the list of classes which this source can output.
     *
     * @return Array of classes that will be output by the source.
     * Null or empty array if it can produce any type of class.
     */
    @Override
    public Class[] getOutputEventClasses() {
        return new Class[]{Map.class, HashMap.class};
    }

    /**
     * Initially Called to connect to the debezium embedded engine to receive change data events asynchronously.
     */
    @Override
    public void connect(ConnectionCallback connectionCallback) {
        changeDataCapture.captureChanges(operation);
    }

    /**
     * This method can be called when it is needed to disconnect from the end point.
     */
    @Override
    public void disconnect() {
        changeDataCapture.stopEngine();
    }

    /**
     * Called at the end to clean all the resources consumed by the {@link Source}
     */
    @Override
    public void destroy() {
    }

    /**
     * Called to pause event consumption
     */
    @Override
    public void pause() {

    }

    /**
     * Called to resume event consumption
     */
    @Override
    public void resume() {

    }

    /**
     * Used to collect the serializable state of the processing element, that need to be
     * persisted for the reconstructing the element to the same state on a different point of time
     *
     * @return stateful objects of the processing element as a map
     */

    @Override
    public synchronized Map<String, Object> currentState() {
        Map<String, Object> currentState = new HashMap<>();

        currentState.put("cacheObj", cache);

        return currentState;
    }

    /**
     * Used to restore serialized state of the processing element, for reconstructing
     * the element to the same state as if was on a previous point of time.
     *
     * @param map the stateful objects of the processing element as a map.
     *            This map will have the  same keys that is created upon calling currentState() method.
     */
    @Override
    public synchronized void restoreState(Map<String, Object> map) {
        Object cacheObj = map.get("cacheObj");
        if (cacheObj instanceof HashMap) {
            this.cache = (HashMap<byte[], byte[]>) cacheObj;
        }
    }

    /**
     * Used to Validate the parameters.
     */
    private void validateParameter() {
        if (!(operation.equals(CDCSourceConstants.INSERT) || operation.equals(CDCSourceConstants.UPDATE)
                || operation.equals(CDCSourceConstants.DELETE))) {
            throw new SiddhiAppValidationException("operation should be one of 'insert', 'update' or 'delete'");
        }
        if (!(commitPolicy.equals(CDCSourceConstants.PERIODIC_OFFSET_COMMIT_POLICY)
                || commitPolicy.equals(CDCSourceConstants.ALWAYS_OFFSET_COMMIT_POLICY))) {
            throw new SiddhiAppValidationException("offset.commit.policy should be PeriodicCommitOffsetPolicy" +
                    " or AlwaysCommitOffsetPolicy");
        }
        if (offsetFileDirectory.isEmpty()) {
            throw new SiddhiAppValidationException("Couldn't set the database.history.file.directory automatically." +
                    " Please set the parameter.");
        } else if (!offsetFileDirectory.endsWith("/")) {
            offsetFileDirectory = offsetFileDirectory + "/";
        }

        if (historyFileDirectory.isEmpty()) {
            throw new SiddhiAppValidationException("Couldn't set the database.history.file.directory automatically." +
                    " Please set the parameter.");
        } else if (!historyFileDirectory.endsWith("/")) {
            historyFileDirectory = historyFileDirectory + "/";
        }

        if (Util.extractDetails(url).get("schema").equals("oracle") && outboundServerName.isEmpty()) {
            throw new SiddhiAppValidationException("database.out.server.name must be given for the oracle database.");
        }

        if (!connectorProperties.isEmpty()) {
            String[] keyValuePairs = connectorProperties.split(",");
            for (String keyValuePair : keyValuePairs) {
                String[] keyValueArray = keyValuePair.split("=");
                try {
                    connectorPropertiesMap.put(keyValueArray[0].trim(), keyValueArray[1].trim());
                } catch (ArrayIndexOutOfBoundsException ex) {
                    throw new SiddhiAppValidationException("connector.properties input is invalid. Check near :" +
                            keyValuePair);
                }
            }
        }
    }
}

