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

import java.util.HashMap;

/**
 * This class Contains methods to store and retrieve the CdcSource objects.
 */
class CDCSourceObjectKeeper {
    private static HashMap<String, CdcSource> objectMap = new HashMap<>();

    /**
     * @param cdcSource is added to the objectMap against it's toString() value.
     */
    static void addCdcObject(CdcSource cdcSource) {
        objectMap.put(cdcSource.toString(), cdcSource);
    }

    /**
     * @param id cdcSource object's toString() value.
     * @return cdcObject if the particular object is already added. Otherwise, return null.
     */
    static CdcSource getCdcObject(String id) {
        return objectMap.get(id);
    }
}
