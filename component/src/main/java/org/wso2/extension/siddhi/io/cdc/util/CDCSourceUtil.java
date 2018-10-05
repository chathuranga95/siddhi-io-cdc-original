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

package org.wso2.extension.siddhi.io.cdc.util;

import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.wso2.extension.siddhi.io.cdc.source.CDCSource;

import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class contains Util methods for the cdc extension.
 */
public class CDCSourceUtil {

    /**
     * Extract the details from the connection url and return as a HashMap.
     * mysql===> jdbc:mysql://hostname:port/testdb
     * Hash map will include a subset of following elements according to the schema:
     * schema
     * host
     * port
     * database name
     *
     * @param url is the connection url given in the siddhi app
     */
    public static Map<String, String> extractDetails(String url) {
        Map<String, String> details = new HashMap<>();
        String host;
        int port;
        String database;

        String[] splittedURL = url.split(":");
        // TODO: 10/4/18 ignoreequals
        if (!splittedURL[0].equals("jdbc")) {
            // TODO: 10/4/18 add the invalid url into the exception
            throw new IllegalArgumentException("Invalid JDBC url.");
        } else {
            switch (splittedURL[1]) {
                case "mysql": {

                    details.put("schema", "mysql");

                    String regex = "jdbc:mysql://(\\w*|[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}):" +
                            "(\\d++)/(\\w*)";
                    Pattern p = Pattern.compile(regex);
                    Matcher matcher = p.matcher(url);
                    if (matcher.find()) {
                        host = matcher.group(1);
                        port = Integer.parseInt(matcher.group(2));
                        database = matcher.group(3);

                    } else {
                        throw new IllegalArgumentException("Invalid JDBC url.");
                    }

                    details.put("database", database);

                    break;
                }
                default:
                    throw new IllegalArgumentException("Unsupported JDBC url.");
            }
            details.put("host", host);
            details.put("port", Integer.toString(port));
        }
        return details;
    }

    /**
     * Create Hash map using the connect record and operation,
     *
     * @param connectRecord is the change data object which is received from debezium embedded engine.
     * @param operation     is the change data event which is specified by the user.
     **/
    // TODO: 10/4/18 return type, Map
    public static Map<String, Object> createMap(ConnectRecord connectRecord, String operation) {
//todo: type of Map
        Map<String, Object> detailsMap = new HashMap<>();
        Struct record = (Struct) connectRecord.value();
        Struct rawDetails;
        List<String> fieldNames = new ArrayList<>();

        //get the change data object's operation.
        String op;
        //todo: serch this get()
        // TODO: 10/5/18 talk to Tishan ayiya
//        op = (String) record.get("op");
        try {
            op = (String) record.get("op");
        } catch (Exception ex) {
            return detailsMap;
        }
// TODO: 10/4/18 equals ignore case, use constants
        //match the change data's operation with user specifying operation and proceed.
        if (operation.equals("insert") && op.equals("c") || operation.equals("delete") && op.equals("d")
                || operation.equals("update") && op.equals("u")) {

            // TODO: 10/4/18 try to merge the switches

            //get the field names of the table
            switch (op) {
                case "c":
                case "u":
                    rawDetails = (Struct) record.get("after");
                    break;
                case "d":
                    rawDetails = (Struct) record.get("before");
                    break;
                default:
                    return detailsMap;
            }
            List<Field> fields = rawDetails.schema().fields();
            for (Field key : fields) {
                fieldNames.add(key.name());
            }

            switch (operation) {
                case "insert":
                    for (String field : fieldNames) {
                        detailsMap.put(field, rawDetails.get(field));
                    }
                    break;
                case "delete":
                    for (String field : fieldNames) {
                        detailsMap.put("before_" + field, rawDetails.get(field));
                    }
                    break;
                case "update":
                    for (String field : fieldNames) {
                        detailsMap.put(field, rawDetails.get(field));
                    }
                    rawDetails = (Struct) record.get("before");
                    for (String field : fieldNames) {
                        detailsMap.put("before_" + field, rawDetails.get(field));
                    }
                    break;
            }
        }

        return detailsMap;
    }

    /**
     * Get the WSO2 Stream Processor's local path from System Variables.
     * if carbon.home is not set, return the current project path. (for test cases only)
     */
    public static String getStreamProcessorPath() {
        String path = System.getProperty("carbon.home");
        // TODO: 10/4/18 move this code into test utils
        if (path == null) {
            path = CDCSource.class.getProtectionDomain().getCodeSource().getLocation().getPath();
            String decodedPath;
            try {
                decodedPath = URLDecoder.decode(path, "UTF-8");
            } catch (Exception ex) {
                return "";
            }

            int x = decodedPath.length() - 1;
            int folderUpCharacterCount = 0;
            int counter = 0;
            while (folderUpCharacterCount < 2) {
                if (Character.toString(decodedPath.charAt(x - counter)).equals("/")) {
                    folderUpCharacterCount++;
                }
                counter++;
            }

            decodedPath = decodedPath.substring(0, x - counter + 2);
            return decodedPath;
        }
        return path;
    }
}
