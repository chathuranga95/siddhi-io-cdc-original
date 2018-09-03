package org.wso2.extension.siddhi.io.cdc.util;

import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.wso2.extension.siddhi.io.cdc.source.CdcSource;

import java.net.URLDecoder;
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
     * Hash map will include a subset of following elements according to the schema:
     * schema
     * host
     * port
     * database name
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
            switch (splittedURL[1]) {
                case "mysql": {

                    details.put("schema", "mysql");

                    String regex = "jdbc:mysql://(\\w*|[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}):(\\d++)/(\\w*)";
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
                case "oracle": {

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
                            throw new IllegalArgumentException("Invalid oracle JDBC url.");
                        }
                        details.put("service", service);
                    }

                    details.put("driver", driver);

                    break;
                }
                default:
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

        //TODO: just drop it if the user is not asking

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

    /**
     * Get the WSO2 Stream Processor's local path.
     */
    // TODO: 8/31/18 discuss this with suho
    public static String getStreamProcessorPath() {
        String path = CdcSource.class.getProtectionDomain().getCodeSource().getLocation().getPath();
        String decodedPath;
        try {
            decodedPath = URLDecoder.decode(path, "UTF-8");
        } catch (Exception ex) {
            return "";
        }

        int x = decodedPath.length() - 1;
        int folderUpCharacterCount = 0;
        int counter = 0;
        while (folderUpCharacterCount < 4) {
            if (Character.toString(decodedPath.charAt(x - counter)).equals("/")) {
                folderUpCharacterCount++;
            }
            counter++;
        }

        decodedPath = decodedPath.substring(0, x - counter + 2);
        return decodedPath;
    }

}
