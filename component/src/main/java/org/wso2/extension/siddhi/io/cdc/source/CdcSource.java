package org.wso2.extension.siddhi.io.cdc.source;

import org.apache.log4j.Logger;
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
 * Extension to the WSO2 Stream Processor to retrieve Database Changes.
 **/
@Extension(
        name = "cdc",
        namespace = "source",
        description = "A user can get real time change data events(INSERT, UPDATE, DELETE) with row data with cdc " +
                "source",
        parameters = {
                @Parameter(
                        name = "username",
                        description = "Username as mentioned in the configuration for the database schema",
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
                        description = "interested change event name. insert, update or delete",
                        type = DataType.STRING
                ),
                @Parameter(name = "name",
                        description = "Unique name for the connector instance.",
                        optional = true,
                        type = DataType.STRING,
                        defaultValue = "<SiddhiAppName>_<StreamName>"
                ),
                @Parameter(name = "offset.file.directory",
                        description = "Path to store the file with the connector’s change data offsets.",
                        optional = true,
                        type = DataType.STRING,
                        defaultValue = "path/to/wso2/sp/cdc/offset/SiddhiAppName"
                ),
                @Parameter(name = "offset.commit.policy",
                        description = "The name of the Java class of the commit policy. When this policy triggers," +
                                " data offsets will be flushed.",
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
                                " the offset data will restored to be commited in a future attempt.",
                        optional = true,
                        type = DataType.STRING,
                        defaultValue = "5000"
                ),
                @Parameter(name = "database.history.file.directory",
                        description = "Path to store database schema history changes.",
                        optional = true,
                        type = DataType.STRING,
                        defaultValue = "path/to/wso2/sp/cdc/history/SiddhiAppName"
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
                        defaultValue = "random"
                ),
                @Parameter(name = "database.out.server.name",
                        description = "Oracle Xstream outbound server name for Oracle. Required for Oracle database",
                        optional = true,
                        type = DataType.STRING,
                        defaultValue = "xstrmServer"
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
        examples = {
                @Example(
                        syntax = " ",
                        description = " "
                )
        }
)

// for more information refer https://wso2.github.io/siddhi/documentation/siddhi-4.0/#sources
public class CdcSource extends Source {

    private static final Logger LOG = Logger.getLogger(CdcSource.class);
    private int flushInterval;
    private int serverID;
    private String serverName;
    private String outboundServerName;
    private String dbName;
    private String pdbName;
    private String siddhiAppName;
    private String url;
    private String streamName;
    private String tableName;
    private String username;
    private String password;
    private String operation;
    private String offsetFileDirectory;
    private String commitPolicy;
    private ChangeDataCapture changeDataCapture;
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

        siddhiAppName = siddhiAppContext.getName();
        streamName = sourceEventListener.getStreamDefinition().getId();

        //initialize mandatory parameters
        url = optionHolder.validateAndGetStaticValue(CDCSourceConstants.DATABASE_CONNECTION_URL,
                CDCSourceConstants.EMPTY_STRING);
        tableName = optionHolder.validateAndGetStaticValue(CDCSourceConstants.TABLE_NAME,
                CDCSourceConstants.EMPTY_STRING);
        username = optionHolder.validateAndGetStaticValue(CDCSourceConstants.USERNAME,
                CDCSourceConstants.EMPTY_STRING);
        password = optionHolder.validateAndGetStaticValue(CDCSourceConstants.PASSWORD,
                CDCSourceConstants.EMPTY_STRING);
        operation = optionHolder.validateAndGetStaticValue(CDCSourceConstants.OPERATION,
                CDCSourceConstants.EMPTY_STRING);

        //initialize optional parameters
        //TODO: get the wso2 carbon home and assign to the default value
        offsetFileDirectory = optionHolder.validateAndGetStaticValue(CDCSourceConstants.OFFSET_FILE_DIRECTORY,
                "/home/chathuranga/mysqlLogs/");
        commitPolicy = optionHolder.validateAndGetStaticValue(CDCSourceConstants.OFFSET_COMMIT_POLICY,
                "PeriodicCommitOffsetPolicy");
        flushInterval = Integer.parseInt(optionHolder.validateAndGetStaticValue(
                CDCSourceConstants.OFFSET_FLUSH_INTERVALMS, "60000"));
        serverID = Integer.parseInt(optionHolder.validateAndGetStaticValue(CDCSourceConstants.DATABASE_SERVER_ID,
                "-1"));
        serverName = optionHolder.validateAndGetStaticValue(CDCSourceConstants.DATABASE_SERVER_NAME,
                CDCSourceConstants.EMPTY_STRING);
        outboundServerName = optionHolder.validateAndGetStaticValue(CDCSourceConstants.DATABASE_OUT_SERVER_NAME,
                "xstrmServer");
        dbName = optionHolder.validateAndGetStaticValue(CDCSourceConstants.DATABASE_DBNAME,
                CDCSourceConstants.EMPTY_STRING);
        pdbName = optionHolder.validateAndGetStaticValue(CDCSourceConstants.DATABASE_PDB_NAME,
                CDCSourceConstants.EMPTY_STRING);
        validateParameter();

        this.changeDataCapture = new ChangeDataCapture();
        if (changeDataCapture.setConfig(username, password, url, tableName, offsetFileDirectory, siddhiAppName,
                streamName, commitPolicy, flushInterval, serverID, serverName,
                outboundServerName, dbName, pdbName)) {
            LOG.info("Config accepted!");
            changeDataCapture.setSourceEventListener(sourceEventListener);
        //    changeDataCapture.captureChanges(operation);
        } else {
            LOG.error("Config rejected!");
            throw new SiddhiAppCreationException("Configuration rejected!");
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
    @Override
    public void connect(ConnectionCallback connectionCallback) throws ConnectionUnavailableException {
        changeDataCapture.captureChanges(operation);
    }

    /**
     * This method can be called when it is needed to disconnect from the end point.
     */
    @Override
    public void disconnect() {

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
//        Map<String, Object> currentState = new HashMap<>();
//        currentState.put("abc", "abcvalue");
//        return currentState;
        return null;
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

    }

    private void validateParameter() {
        //TODO: write parameter validation. throw the exceptions whenever needed.
        if (url.isEmpty()) {
            throw new SiddhiAppValidationException("url must be given.");
        }
        if (username.isEmpty()) {
            throw new SiddhiAppValidationException("username must be given.");
        }
        if (password.isEmpty()) {
            throw new SiddhiAppValidationException("password must be given.");
        }
        if (tableName.isEmpty()) {
            throw new SiddhiAppValidationException("table.name must be given.");
        }
        if (!(operation.equals(CDCSourceConstants.INSERT) || operation.equals(CDCSourceConstants.UPDATE)
                || operation.equals(CDCSourceConstants.DELETE))) {
            throw new SiddhiAppValidationException("operation should be one of 'insert', 'update' or 'delete'");
        }
    }
}

