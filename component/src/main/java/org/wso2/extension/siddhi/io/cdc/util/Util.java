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
 * This class contains Util methods for the cdc extension.
 */
public class Util {

    /**
     * Extract the details from the connection url and return as an array.
     * <p>
     * mysql===> jdbc:mysql://hostname:port/testdb
     * oracle==> jdbc:oracle:thin:@hostname:port:SID
     * or
     * jdbc:oracle:thin:@hostname:port/SERVICE
     * <p>
     * Hash map will include a subset of following elements according to the schema:
     * schema
     * host
     * port
     * database name
     * SID
     * driver
     *
     * @param url is the connection url given in the siddhi app
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
                    throw new IllegalArgumentException("Unsupported JDBC url.");
            }
            details.put("host", host);
            details.put("port", Integer.toString(port));
        }
        return details;
    }

    /**
     * Create Hash map using the connect record,
     *
     * @param connectRecord is the change data object which is received from debezium embedded engine.
     * @param operation     is the change data event which is specified by the user.
     **/
    public static HashMap<String, String> createMap(ConnectRecord connectRecord, String operation) {

        HashMap<String, String> detailsMap = new HashMap<>();
        Struct record = (Struct) connectRecord.value();
        Struct rawDetails;
        List<String> fieldNames = new ArrayList<>();

        //get the change data object's operation.
        String op;
        try {
            op = (String) record.get("op");
        } catch (Exception ex) {
            return detailsMap;
        }


        //match the change data's operation with user specifying operation and proceed.
        if (operation.equals("insert") && op.equals("c") || operation.equals("delete") && op.equals("d")
                || operation.equals("update") && op.equals("u")) {

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
                        detailsMap.put(field, (String) rawDetails.get(field));
                    }
                    break;
                case "delete":
                    for (String field : fieldNames) {
                        detailsMap.put("before_" + field, (String) rawDetails.get(field));
                    }
                    break;
                case "update":
                    for (String field : fieldNames) {
                        detailsMap.put(field, (String) rawDetails.get(field));
                    }
                    rawDetails = (Struct) record.get("before");
                    for (String field : fieldNames) {
                        detailsMap.put("before_" + field, (String) rawDetails.get(field));
                    }
                    break;
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


    /**
     * Convert HashMap<byte[],byte[]> to String and vice versa.
     **/
    public static String mapToString(HashMap<byte[], byte[]> map) {
        StringBuilder outStr = new StringBuilder("{");

        for (Map.Entry<byte[], byte[]> entry : map.entrySet()) {
            outStr.append(byteArrayToString(entry.getKey())).append(":").append(byteArrayToString(entry.getValue()))
                    .append(";");
        }

        outStr = new StringBuilder(outStr.substring(0, outStr.length() - 1));
        outStr.append("}");
        return outStr.toString();
    }

    private static String byteArrayToString(byte[] arr) {
        StringBuilder outStr = new StringBuilder("{");

        for (byte num : arr) {
            outStr.append(num).append(",");
        }

        outStr = new StringBuilder(outStr.substring(0, outStr.length() - 1));
        outStr.append("}");

        return outStr.toString();
    }

    public static HashMap<byte[], byte[]> stringToMap(String str) {
        HashMap<byte[], byte[]> map = new HashMap<>();

        if (str.isEmpty()) {
            return map;
        }

        str = str.substring(1, str.length() - 1);
        String[] keyValuePairs = str.split(";");

        for (String pair : keyValuePairs) {
            String[] entry = pair.split(":");
            map.put(stringToByteArr(entry[0].trim()), stringToByteArr(entry[1].trim()));
        }


        return map;
    }

    private static byte[] stringToByteArr(String str) {
        str = str.substring(1, str.length() - 1);
        String[] nums = str.split(",");
        byte[] byteArr = new byte[nums.length];
        for (int i = 0; i < nums.length; i++) {
            byteArr[i] = Byte.parseByte(nums[i]);
        }

        return byteArr;
    }

}
