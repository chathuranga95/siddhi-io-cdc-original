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

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.storage.MemoryOffsetBackingStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.extension.siddhi.io.cdc.util.CDCSourceConstants;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * This class saves and loads the change data offsets in in-memory.
 */
public class InMemoryOffsetBackingStore extends MemoryOffsetBackingStore {
    private static final Logger log = LoggerFactory.getLogger(InMemoryOffsetBackingStore.class);
    private volatile CDCSource cdcSource = null;
    private Map<byte[], byte[]> cache = new HashMap<>();

    public InMemoryOffsetBackingStore() {
    }

    /**
     * Configure this InMemoryOffsetBackingStore.
     * initialize cdcSource with pre set config.
     */
    public void configure(WorkerConfig config) {
        super.configure(config);
        int cdcSourceObjectId = Integer.parseInt((String) config.originals().get(CDCSourceConstants.CDC_SOURCE_OBJECT));
        cdcSource = CDCSourceObjectKeeper.getCdcSourceObjectKeeper().getCdcObject(cdcSourceObjectId);
    }

    public synchronized void start() {
        // TODO: 10/4/18 use locks to synchronize
        super.start();
        log.debug("Started InMemoryOffsetBackingStore");

        //Load offsets from Snapshot.
//        this.data = cdcSource.getOffsetData();
        cache = cdcSource.getOffsetData();

        try {
            this.data = new HashMap<>();

            for (Object obj : cache.entrySet()) {
                Map.Entry<byte[], byte[]> mapEntry = (Map.Entry) obj;
                ByteBuffer key = mapEntry.getKey() != null ? ByteBuffer.wrap(mapEntry.getKey()) : null;
                ByteBuffer value = mapEntry.getValue() != null ? ByteBuffer.wrap(mapEntry.getValue()) : null;
                this.data.put(key, value);
            }
        } catch (Exception ex) {
            log.error("error loading the in-memory offsets.", ex);
        }

    }

    public void stop() {
        super.stop();
        log.debug("Stopped InMemoryOffsetBackingStore");
    }

    /**
     * Send offsets to cdcSource to save in next Snapshot.
     */
    @Override
    protected synchronized void save() {
//        cdcSource.setOffsetData(this.data);
        try {

            for (Object o : this.data.entrySet()) {
                Map.Entry mapEntry = (Map.Entry) o;
                byte[] key = mapEntry.getKey() != null ? ((ByteBuffer) mapEntry.getKey()).array() : null;
                byte[] value = mapEntry.getValue() != null ? ((ByteBuffer) mapEntry.getValue()).array() : null;
                cache.put(key, value);
            }
            cdcSource.setCache(cache);

        } catch (Exception var7) {
            throw new ConnectException(var7);
        }
    }
}
