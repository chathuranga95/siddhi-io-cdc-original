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

/**
 * CDC source constants
 */

public class CDCSourceConstants {
    public static final String USERNAME = "username";
    public static final String PASSWORD = "password";
    public static final String DATABASE_CONNECTION_URL = "url";
    public static final String OPERATION = "operation";
    public static final String OFFSET_FILE_DIRECTORY = "offsets.file.directory";
    public static final String DATABASE_HISTORY_FILE_DIRECTORY = "database.history.file.directory";
    public static final String OFFSET_COMMIT_POLICY = "offsets.commit.policy";
    public static final String PERIODIC_OFFSET_COMMIT_POLICY = "PeriodicCommitOffsetPolicy";
    public static final String ALWAYS_OFFSET_COMMIT_POLICY = "AlwaysCommitOffsetPolicy";
    public static final String OFFSET_FLUSH_INTERVALMS = "offsets.flush.intervalms";
    public static final String DATABASE_SERVER_NAME = "database.server.name";
    public static final String DATABASE_SERVER_ID = "database.server.id";
    public static final String DATABASE_OUT_SERVER_NAME = "database.out.server.name";
    public static final String TABLE_NAME = "table.name";
    public static final String DATABASE_DBNAME = "database.dbname";
    public static final String DATABASE_PDB_NAME = "database.pdb.name";
    public static final String CONNECTOR_PROPERTIES = "connector.properties";
    public static final String EMPTY_STRING = "";
    public static final String INSERT = "insert";
    public static final String UPDATE = "update";
    public static final String DELETE = "delete";
}