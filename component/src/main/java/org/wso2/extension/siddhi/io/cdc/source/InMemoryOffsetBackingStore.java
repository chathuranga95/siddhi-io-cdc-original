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

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * In Memory offset store implementation.
 */
public class InMemoryOffsetBackingStore extends MemoryOffsetBackingStore {
    private static final Logger logger = LoggerFactory.getLogger(InMemoryOffsetBackingStore.class);
    private HashMap<byte[], byte[]> cache;

    public InMemoryOffsetBackingStore() {
    }

    public void configure(WorkerConfig config) {
        super.configure(config);
        this.cache = new HashMap<>();
    }

    public synchronized void start() {
        super.start();
        logger.info("Starting InMemoryOffsetBackingStore");
        this.load();
    }

    public synchronized void stop() {
        super.stop();
        logger.info("Stopped InMemoryOffsetBackingStore");
    }

    private void load() {
        logger.info("load called...");
        try {
            this.data = new HashMap();
            Iterator i$ = cache.entrySet().iterator();

            while (i$.hasNext()) {
                Map.Entry<byte[], byte[]> mapEntry = (Map.Entry) i$.next();
                ByteBuffer key = mapEntry.getKey() != null ? ByteBuffer.wrap((byte[]) mapEntry.getKey()) : null;
                ByteBuffer value = mapEntry.getValue() != null ? ByteBuffer.wrap((byte[]) mapEntry.getValue()) : null;
                this.data.put(key, value);
            }
        } catch (Exception ex) {
            logger.error(ex.toString());
        }
    }

    protected void save() {
        logger.info("save called...");
        try {
            for (Object o : this.data.entrySet()) {
                Map.Entry mapEntry = (Map.Entry) o;
                byte[] key = mapEntry.getKey() != null ? ((ByteBuffer) mapEntry.getKey()).array() : null;
                byte[] value = mapEntry.getValue() != null ? ((ByteBuffer) mapEntry.getValue()).array() : null;
                cache.put(key, value);
            }
        } catch (Exception var7) {
            throw new ConnectException(var7);
        }
    }
}
