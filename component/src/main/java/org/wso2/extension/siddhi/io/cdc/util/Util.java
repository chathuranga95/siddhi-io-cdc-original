package org.wso2.extension.siddhi.io.cdc.util;

import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Some util methods.
 */
public class Util {

    /**
     * Extract the details from the connection url and return as an array.
     * <p>
     * mysql===> jdbc:mysql://hostname:port/testdb
     * oracle==> jdbc:oracle:thin:@hostname:port:SID
     * or
     * jdbc:oracle:thin:@hostname:port/SERVICE
     * sqlserver => jdbc:sqlserver://hostname:port;databaseName=testdb
     * postgres ==> jdbc:postgresql://hostname:port/testdb
     * <p>
     * <p>
     * The elements in the hash-map:
     * schema
     * host
     * port
     * database name:
     * SID
     * driver
     */
    public static Map<String, String> extractDetails(String url) {
        Map<String, String> details = new HashMap<>();
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

                details.put("schema", "mysql");

                String regex = "jdbc:mysql://(\\w*|[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}):(\\d++)/(\\w*)";
                Pattern p = Pattern.compile(regex);
                Matcher matcher = p.matcher(url);
                if (matcher.find()) {
                    host = matcher.group(1);
                    port = Integer.parseInt(matcher.group(2));
                    database = matcher.group(3);

                } else {
                    // handle error appropriately
                    throw new IllegalArgumentException("Invalid JDBC url.");
                }


                details.put("database", database);

            } else if (splittedURL[1].equals("oracle")) {

                details.put("schema", "oracle");

                String regex = "jdbc:oracle:(thin|oci):@(\\w*|[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}):" +
                        "(\\d++):(\\w*)";
                Pattern p = Pattern.compile(regex);

                Matcher matcher = p.matcher(url);
                if (matcher.find()) {
                    driver = matcher.group(1);
                    host = matcher.group(2);
                    port = Integer.parseInt(matcher.group(3));
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
                        port = Integer.parseInt(matcherService.group(3));
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
            details.put("port", Integer.toString(port));
        }
        return details;
    }

    /**
     * Create Hash map using the connect record
     **/
    public static HashMap<String, String> createMap(ConnectRecord connectRecord, String operation) {

        HashMap<String, String> detailsMap = new HashMap<>();
        Struct record = (Struct) connectRecord.value();
        Struct rawDetails;
        List<String> fieldNames = new ArrayList<>();

        //get the operation from the record
        String op;
        try {
            op = (String) record.get("op");
        } catch (Exception ex) {
            return detailsMap;
        }

        //get the field names of the table
        if (op.equals("c") || op.equals("u")) {
            rawDetails = (Struct) record.get("after");
        } else if (op.equals("d")) {
            rawDetails = (Struct) record.get("before");
        } else {
            return detailsMap;
        }

        List<Field> fields = rawDetails.schema().fields();
        for (Field key : fields) {
            fieldNames.add(key.name());
        }

        if (operation.equals("insert") && op.equals("c")) {

            rawDetails = (Struct) record.get("after");

            for (String field : fieldNames) {
                detailsMap.put(field, (String) rawDetails.get(field));
            }
        } else if (operation.equals("delete") && op.equals("d")) {
            rawDetails = (Struct) record.get("before");

            for (String field : fieldNames) {
                detailsMap.put("before_" + field, (String) rawDetails.get(field));
            }
        } else if (operation.equals("update") && op.equals("u")) {
            rawDetails = (Struct) record.get("after");

            for (String field : fieldNames) {
                detailsMap.put(field, (String) rawDetails.get(field));
            }

            rawDetails = (Struct) record.get("before");

            for (String field : fieldNames) {
                detailsMap.put("before_" + field, (String) rawDetails.get(field));
            }
        }

        return detailsMap;
    }

}
