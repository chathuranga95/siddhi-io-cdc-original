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

/**
 * CDC source constants
 * */

class CDCSourceConstants {
    static final String USERNAME = "username";
    static final String PASSWORD = "password";
    static final String DATABASE_CONNECTION_URL = "url";
    static final String OPERATION = "operation";
    static final String OFFSET_FILE_DIRECTORY = "offset.file.directory";
    static final String DATABASE_HISTORY_FILE_DIRECTORY = "database.history.file.directory";
    static final String OFFSET_COMMIT_POLICY = "offset.commit.policy";
    static final String PERIODIC_OFFSET_COMMIT_POLICY = "PeriodicCommitOffsetPolicy";
    static final String ALWAYS_OFFSET_COMMIT_POLICY = "AlwaysCommitOffsetPolicy";
    static final String OFFSET_FLUSH_INTERVALMS = "offset.flush.intervalms";
    static final String DATABASE_SERVER_NAME = "database.server.name";
    static final String DATABASE_SERVER_ID = "database.server.id";
    static final String DATABASE_OUT_SERVER_NAME = "database.out.server.name";
    static final String TABLE_NAME = "table.name";
    static final String DATABASE_DBNAME = "database.dbname";
    static final String DATABASE_PDB_NAME = "database.pdb.name";
    static final String EMPTY_STRING = "";
    static final String INSERT = "insert";
    static final String UPDATE = "update";
    static final String DELETE = "delete";
}
