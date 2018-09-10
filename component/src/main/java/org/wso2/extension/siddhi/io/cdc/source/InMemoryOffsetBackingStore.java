//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package org.wso2.extension.siddhi.io.cdc.source;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.storage.MemoryOffsetBackingStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.extension.siddhi.io.cdc.util.Util;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.prefs.Preferences;

public class InMemoryOffsetBackingStore extends MemoryOffsetBackingStore {
    private static final Logger log = LoggerFactory.getLogger(InMemoryOffsetBackingStore.class);
    HashMap<byte[], byte[]> cache = new HashMap<>();

    public InMemoryOffsetBackingStore() {
    }

    public void configure(WorkerConfig config) {
        super.configure(config);
    }

    public synchronized void start() {
        super.start();
        this.load();
    }

    public synchronized void stop() {
        super.stop();
        log.info("Stopped FileOffsetBackingStore");
    }

    private void load() {
        log.info("Load called...");
        Preferences prefs = Preferences.userNodeForPackage(InMemoryOffsetBackingStore.class);
        cache = Util.stringToMap(prefs.get("mMapInstance", ""));
        try {
            this.data = new HashMap();
            Iterator iterator = cache.entrySet().iterator();

            while (iterator.hasNext()) {
                Map.Entry<byte[], byte[]> mapEntry = (Map.Entry) iterator.next();
                ByteBuffer key = mapEntry.getKey() != null ? ByteBuffer.wrap((byte[]) mapEntry.getKey()) : null;
                ByteBuffer value = mapEntry.getValue() != null ? ByteBuffer.wrap((byte[]) mapEntry.getValue()) : null;
                this.data.put(key, value);
            }
        } catch (Exception ex) {
        }
    }

    protected void save() {
        log.info("save called...");
        Preferences prefs = Preferences.userNodeForPackage(InMemoryOffsetBackingStore.class);
        try {
            for (Object o : this.data.entrySet()) {
                Map.Entry mapEntry = (Map.Entry) o;
                byte[] key = mapEntry.getKey() != null ? ((ByteBuffer) mapEntry.getKey()).array() : null;
                byte[] value = mapEntry.getValue() != null ? ((ByteBuffer) mapEntry.getValue()).array() : null;
                cache.put(key, value);
                prefs.put("mMapInstance", Util.MapToString(cache));

            }
        } catch (Exception var7) {
            throw new ConnectException(var7);
        }
    }
}
