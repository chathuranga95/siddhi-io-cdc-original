# API Docs - v1.0.0-SNAPSHOT

## Source

### cdc *<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#source">(Source)</a>*

<p style="word-wrap: break-word">A user can get real time change data events(INSERT, UPDATE, DELETE) with row data with cdc source</p>

<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>
```
@source(type="cdc", username="<STRING>", password="<STRING>", url="<STRING>", table.name="<STRING>", operation="<STRING>", name="<STRING>", offset.file.directory="<STRING>", offset.commit.policy="<STRING>", offset.flush.intervalms="<STRING>", offset.flush.timeout.ms="<STRING>", database.history.file.directory="<STRING>", database.server.name="<STRING>", database.server.id="<STRING>", database.out.server.name="<STRING>", database.dbname="<STRING>", database.pdb.name="<STRING>", @map(...)))
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">username</td>
        <td style="vertical-align: top; word-wrap: break-word">Username as mentioned in the configuration for the database schema</td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">password</td>
        <td style="vertical-align: top; word-wrap: break-word">Password for the user</td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">url</td>
        <td style="vertical-align: top; word-wrap: break-word">Connection url to the database.use format:for mysql--&gt; jdbc:mysql://&lt;host&gt;:&lt;port&gt;/&lt;database_name&gt; for oracle--&gt; jdbc:oracle:&lt;driver&gt;:@&lt;host&gt;:&lt;port&gt;:&lt;SID&gt;</td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">table.name</td>
        <td style="vertical-align: top; word-wrap: break-word">Name of the table which needs to be monitored for data changes</td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">operation</td>
        <td style="vertical-align: top; word-wrap: break-word">interested change event name. insert, update or delete</td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">name</td>
        <td style="vertical-align: top; word-wrap: break-word">Unique name for the connector instance.</td>
        <td style="vertical-align: top"><SiddhiAppName>_<StreamName></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">offset.file.directory</td>
        <td style="vertical-align: top; word-wrap: break-word">Path to store the file with the connector’s change data offsets.</td>
        <td style="vertical-align: top">path/to/wso2/sp/cdc/offset/SiddhiAppName</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">offset.commit.policy</td>
        <td style="vertical-align: top; word-wrap: break-word">The name of the Java class of the commit policy. When this policy triggers, data offsets will be flushed.</td>
        <td style="vertical-align: top">PeriodicCommitOffsetPolicy</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">offset.flush.intervalms</td>
        <td style="vertical-align: top; word-wrap: break-word">Time in milliseconds to flush offsets when the commit policy is set to File</td>
        <td style="vertical-align: top">60000</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">offset.flush.timeout.ms</td>
        <td style="vertical-align: top; word-wrap: break-word">Maximum number of milliseconds to wait for records to flush. On timeout, the offset data will restored to be commited in a future attempt.</td>
        <td style="vertical-align: top">5000</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">database.history.file.directory</td>
        <td style="vertical-align: top; word-wrap: break-word">Path to store database schema history changes.</td>
        <td style="vertical-align: top">path/to/wso2/sp/cdc/history/SiddhiAppName</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">database.server.name</td>
        <td style="vertical-align: top; word-wrap: break-word">Logical name that identifies and provides a namespace for the particular database server</td>
        <td style="vertical-align: top"><host>:<port></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">database.server.id</td>
        <td style="vertical-align: top; word-wrap: break-word">For MySQL, a unique integer between 1 to 2^32 as the ID, This is used when joining MySQL database cluster to read binlog</td>
        <td style="vertical-align: top">random</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">database.out.server.name</td>
        <td style="vertical-align: top; word-wrap: break-word">Oracle Xstream outbound server name for Oracle. Required for Oracle database</td>
        <td style="vertical-align: top">xstrmServer</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">database.dbname</td>
        <td style="vertical-align: top; word-wrap: break-word">Name of the database to connect to. Must be the CDB name when working with the CDB + PDB model.</td>
        <td style="vertical-align: top"><sid></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">database.pdb.name</td>
        <td style="vertical-align: top; word-wrap: break-word">Name of the PDB to connect to. Required when working with the CDB + PDB model.</td>
        <td style="vertical-align: top">ORCLPDB1</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
 
```
<p style="word-wrap: break-word"> </p>

