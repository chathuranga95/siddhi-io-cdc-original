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
    private static final Logger LOG = LoggerFactory.getLogger(InMemoryOffsetBackingStore.class);
    private volatile CdcSource cdcSource = null;
    private HashMap<byte[], byte[]> cache = new HashMap<>();

    public InMemoryOffsetBackingStore() {
    }

    public void configure(WorkerConfig config) {
        super.configure(config);
        String cdcSourceObjectId = (String) config.originals().get(CDCSourceConstants.CDC_SOURCE_OBJECT);
        cdcSource = CDCSourceObjectKeeper.getCdcObject(cdcSourceObjectId);
    }

    public synchronized void start() {
        super.start();
        LOG.info("Started InMemoryOffsetBackingStore");
        this.load();
    }

    public synchronized void stop() {
        super.stop();
        LOG.info("Stopped InMemoryOffsetBackingStore");
    }

    private synchronized void load() {
        cache = cdcSource.getCache();

        try {
            this.data = new HashMap<>();

            for (Object obj : cache.entrySet()) {
                Map.Entry<byte[], byte[]> mapEntry = (Map.Entry) obj;
                ByteBuffer key = mapEntry.getKey() != null ? ByteBuffer.wrap(mapEntry.getKey()) : null;
                ByteBuffer value = mapEntry.getValue() != null ? ByteBuffer.wrap(mapEntry.getValue()) : null;
                this.data.put(key, value);
            }
        } catch (Exception ex) {
            LOG.error("error loading the in-memory offsets.", ex);
        }
    }

    protected synchronized void save() {
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
//
//    /**
//     * Used to collect the serializable state of the processing element, that need to be
//     * persisted for the reconstructing the element to the same state on a different point of time
//     *
//     * @return stateful objects of the processing element as an array
//     */
//    @Override
//    public synchronized Map<String, Object> currentState() {
//        Map<String, Object> currentState = new HashMap<>();
//        currentState.put("cacheObj", cache);
//        LOG.debug("current state called... " + cache.size());
//
//        return currentState;
//    }
//
//    /**
//     * Used to restore serialized state of the processing element, for reconstructing
//     * the element to the same state as if was on a previous point of time.
//     *
//     * @param state the stateful objects of the element as an array on
//     *              the same order provided by currentState().
//     */
//    @Override
//    public synchronized void restoreState(Map<String, Object> state) {
//        Object cacheObj = state.get("cacheObj");
//        if (cacheObj instanceof HashMap) {
//            this.cache = (HashMap<byte[], byte[]>) cacheObj;
//        }
//        LOG.debug("restore state called... " + cache.size());
//    }
//
//    @Override
//    public String getElementId() {
//        return null;
//    }
}
