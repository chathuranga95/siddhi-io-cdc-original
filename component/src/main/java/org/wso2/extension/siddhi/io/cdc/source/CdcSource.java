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

import org.wso2.extension.siddhi.io.cdc.util.Util;
import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.annotation.Parameter;
import org.wso2.siddhi.annotation.util.DataType;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.exception.ConnectionUnavailableException;
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
                "before_X : The table's column X value before the event occurred. Applicable when 'delete' or 'update'" +
                " operations are specified.\n",
        parameters = {
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
                @Parameter(name = "url",
                        description = "Connection url to the database." +
                                "use format:" +
                                "for mysql--> jdbc:mysql://<host>:<port>/<database_name> " +
                                "for oracle--> jdbc:oracle:<driver>:@<host>:<port>:<SID>",
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
                @Parameter(name = "offset.file.directory",
                        description = "Path to store the file with the connector’s change data offsets.",
                        optional = true,
                        type = DataType.STRING,
                        defaultValue = "{WSO2SP_HOME}/cdc/offset/<SiddhiAppName>"
                ),
                @Parameter(name = "offset.commit.policy",
                        description = "The name of the Java class of the commit policy. When this policy triggers," +
                                " data offsets will be flushed. Should be one of 'PeriodicCommitOffsetPolicy' or " +
                                "AlwaysCommitOffsetPolicy",
                        optional = true,
                        type = DataType.STRING,
                        defaultValue = "PeriodicCommitOffsetPolicy"
                ),
                @Parameter(name = "offset.flush.intervalms",
                        description = "Time in milliseconds to flush offsets when the commit policy is set to File",
                        optional = true,
                        type = DataType.STRING,
                        defaultValue = "60000"
                ),
                @Parameter(name = "offset.flush.timeout.ms",
                        description = "Maximum number of milliseconds to wait for records to flush. On timeout," +
                                " the offset data will restored to be committed in a future attempt.",
                        optional = true,
                        type = DataType.STRING,
                        defaultValue = "5000"
                ),

                //TODO: check whether we can keep the file in the carbon home or not.
                @Parameter(name = "database.history.file.directory",
                        description = "Path to store database schema history changes.",
                        optional = true,
                        type = DataType.STRING,
                        defaultValue = "{WSO2SP_HOME}/cdc/history/<SiddhiAppName>"
                ),
                @Parameter(name = "database.server.name",
                        description = "Logical name that identifies and provides a namespace for the " +
                                "particular database server",
                        optional = true,
                        type = DataType.STRING,
                        defaultValue = "<host>:<port>"
                ),
                @Parameter(name = "database.server.id",
                        description = "For MySQL, a unique integer between 1 to 2^32 as the ID," +
                                " This is used when joining MySQL database cluster to read binlog",
                        optional = true,
                        type = {DataType.STRING},
                        defaultValue = "<random integer between 5400 and 6400>"
                ),
                @Parameter(name = "database.out.server.name",
                        description = "Oracle Xstream outbound server name for Oracle. Required for Oracle database",
                        optional = true,
                        defaultValue = "a",
                        type = DataType.STRING
                ),
                @Parameter(name = "database.dbname",
                        description = "Name of the database to connect to. Must be the CDB name" +
                                " when working with the CDB + PDB model.",
                        optional = true,
                        type = DataType.STRING,
                        defaultValue = "<sid>"
                ),
                @Parameter(name = "database.pdb.name",
                        description = "Name of the PDB to connect to. Required when working with the CDB + PDB model.",
                        optional = true,
                        type = DataType.STRING,
                        defaultValue = "ORCLPDB1"
                ),
        },
//        systemParameter = {
//                @SystemParameter(name = "",
//                        description = "",
//                        defaultValue = "",
//                        possibleParameters = {"", ""})
//        },
        examples = {
                @Example(
                        syntax = "@source(type = 'cdc' , url = 'jdbc:mysql://localhost:3306/SimpleDB'," +
                                " username = 'cdcuser', password = 'pswd4cdc', table.name = 'students'," +
                                " operation = 'insert', @map(type='keyvalue', @attributes(id = 'id', name = 'name')))" +
                                "define stream inputStream (id string, name string);",
                        description = "In this example, the cdc source starts listening to the row insertions" +
                                "on students table which is under MySQL database that" +
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
                                "on students table which is under MySQL database that" +
                                " can be accessed with the given url"
                ),
                @Example(
                        syntax = "@source(type = 'cdc' , url = 'jdbc:mysql://localhost:3306/SimpleDB'," +
                                " username = 'cdcuser', password = 'pswd4cdc', table.name = 'students'," +
                                " operation = 'delete', @map(type='keyvalue', @attributes(before_id = 'before_id'," +
                                " before_name = 'before_name')))" +
                                "define stream inputStream (before_id string, before_name string);",
                        description = "In this example, the cdc source starts listening to the row deletions" +
                                "on students table which is under MySQL database that" +
                                " can be accessed with the given url"
                )
        }
)

// for more information refer https://wso2.github.io/siddhi/documentation/siddhi-4.0/#sources
public class CdcSource extends Source {


    private String outboundServerName;
    private String url;
    private String operation;
    private String offsetFileDirectory;
    private String commitPolicy;
    private ChangeDataCapture changeDataCapture;
    private String historyFileDirectory;


    //TODO: remove the auto generated lines

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
        //TODO: declare the system parameters as necessary
        //configReader.readConfig()
        //annotation, config reader(deployment .yaml file), default value we assigned.
        //check the annotation order in the sp docs, link in the chat

        String siddhiAppName = siddhiAppContext.getName();
        String streamName = sourceEventListener.getStreamDefinition().getId();

        //initialize mandatory parameters
        url = optionHolder.validateAndGetOption(CDCSourceConstants.DATABASE_CONNECTION_URL).getValue();
        String tableName = optionHolder.validateAndGetOption(CDCSourceConstants.TABLE_NAME).getValue();
        String username = optionHolder.validateAndGetOption(CDCSourceConstants.USERNAME).getValue();
        String password = optionHolder.validateAndGetOption(CDCSourceConstants.PASSWORD).getValue();
        operation = optionHolder.validateAndGetOption(CDCSourceConstants.OPERATION).getValue();

        //initialize optional parameters
        offsetFileDirectory = optionHolder.validateAndGetStaticValue(CDCSourceConstants.OFFSET_FILE_DIRECTORY,
                Util.getStreamProcessorPath() + "cdc/offsets/" + siddhiAppName + "/");
        historyFileDirectory = optionHolder.validateAndGetStaticValue(
                CDCSourceConstants.DATABASE_HISTORY_FILE_DIRECTORY,
                Util.getStreamProcessorPath() + "cdc/history/" + siddhiAppName + "/");
        commitPolicy = optionHolder.validateAndGetStaticValue(CDCSourceConstants.OFFSET_COMMIT_POLICY,
                "PeriodicCommitOffsetPolicy");
        int flushInterval = Integer.parseInt(optionHolder.validateAndGetStaticValue(
                CDCSourceConstants.OFFSET_FLUSH_INTERVALMS, "60000"));
        int serverID = Integer.parseInt(optionHolder.validateAndGetStaticValue(CDCSourceConstants.DATABASE_SERVER_ID,
                "-1"));
        String serverName = optionHolder.validateAndGetStaticValue(CDCSourceConstants.DATABASE_SERVER_NAME,
                CDCSourceConstants.EMPTY_STRING);
        outboundServerName = optionHolder.validateAndGetStaticValue(CDCSourceConstants.DATABASE_OUT_SERVER_NAME,
                CDCSourceConstants.EMPTY_STRING);
        String dbName = optionHolder.validateAndGetStaticValue(CDCSourceConstants.DATABASE_DBNAME,
                CDCSourceConstants.EMPTY_STRING);
        String pdbName = optionHolder.validateAndGetStaticValue(CDCSourceConstants.DATABASE_PDB_NAME,
                CDCSourceConstants.EMPTY_STRING);
        validateParameter();


        this.changeDataCapture = new ChangeDataCapture();
        try {
            changeDataCapture.setConfig(username, password, url, tableName, offsetFileDirectory, historyFileDirectory,
                    siddhiAppName, streamName, commitPolicy, flushInterval, serverID, serverName,
                    outboundServerName, dbName, pdbName);
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
     * Initially Called to connect to the end point for start retrieving the messages asynchronously .
     *
     * @param connectionCallback Callback to pass the ConnectionUnavailableException in case of connection failure after
     *                           initial successful connection. (can be used when events are receiving asynchronously)
     * @throws ConnectionUnavailableException if it cannot connect to the source backend immediately.
     */
    // TODO: 9/3/18 work on this exception
    @Override
    public void connect(ConnectionCallback connectionCallback) throws ConnectionUnavailableException {
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
    public Map<String, Object> currentState() {
        Map<String, Object> currentState = new HashMap<>();

        currentState.put("changeDataCaptureObject", changeDataCapture);

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
    public void restoreState(Map<String, Object> map) {
        ChangeDataCapture changeDataCaptureObj = (ChangeDataCapture) map.get("changeDataCaptureObject");
        changeDataCaptureObj.captureChanges(changeDataCaptureObj.operation);
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
    }
}

