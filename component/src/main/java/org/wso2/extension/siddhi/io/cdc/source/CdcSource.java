package org.wso2.extension.siddhi.io.cdc.source;

import org.apache.log4j.Logger;
import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.annotation.Parameter;
import org.wso2.siddhi.annotation.util.DataType;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.exception.ConnectionUnavailableException;
import org.wso2.siddhi.core.stream.input.source.Source;
import org.wso2.siddhi.core.stream.input.source.SourceEventListener;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.core.util.transport.OptionHolder;

import java.net.URI;
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
                @Parameter(name = "url",
                        description = "Connection url to the database." +
                                "use format:" +
                                "for mysql--> jdbc:mysql://<username>:<password>@<host>:<port>/<db_name> " +
                                "for oracle--> jdbc:oracle:<driver>:<username>/<password>@<host>:<port>:<SID>",
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
                        defaultValue = "host:_port_"
                ),
                @Parameter(name = "database.server.id",
                        description = "For MySQL, a unique integer between 1 to 2^32 as the ID," +
                                " This is used when joining MySQL database cluster to read binlog",
                        optional = true,
                        type = DataType.STRING,
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
                        defaultValue = "sid"
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
    /**
     * The initialization method for {@link Source}, will be called before other methods. It used to validate
     * all configurations and to get initial values.
     *
     * @param sourceEventListener After receiving events, the source should trigger onEvent() of this listener.
     * Listener will then pass on the events to the appropriate mappers for processing .
     * @param optionHolder        Option holder containing static configuration related to the {@link Source}
     * @param configReader        ConfigReader is used to read the {@link Source} related system configuration.
     * @param siddhiAppContext    the context of the {@link org.wso2.siddhi.query.api.SiddhiApp} used to get Siddhi
     * related utility functions.
     */

    private String url = "url";
    private String username = "username";
    private String password = "password";
    private String host = "host";
    private String port = "port";

    @Override
    public void init(SourceEventListener sourceEventListener, OptionHolder optionHolder,
                     String[] requestedTransportPropertyNames, ConfigReader configReader,
                     SiddhiAppContext siddhiAppContext) {

        //siddhiAppContext.getName(); //to get the Siddhi app name

        this.url = optionHolder.validateAndGetStaticValue(CDCSourceConstants.DATABASE_CONNECTION_URL);
        String[] urlElements = url.split(":");


//        String url = "jdbc:mysql://localhost:3306/SimpleDB";
        String cleanURI = url.substring(5);

//        sourceEventListener.getStreamDefinition().getId(); //to get the siddhi stream id (possibly the name)

        URI uri = URI.create(cleanURI);

        LOG.info("you have given database " + uri.getScheme() + " host " + uri.getHost() + " port " + uri.getPort());
    }

    /**
     * Returns the list of classes which this source can output.
     *
     * @return Array of classes that will be output by the source.
     * Null or empty array if it can produce any type of class.
     */
    @Override
    public Class[] getOutputEventClasses() {
        return new Class[0];
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
}

