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

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;


/**
 * This class saves and loads the change data event offsets in in-memory.
 */
public class InMemoryOffsetBackingStore extends MemoryOffsetBackingStore {
    private static final Logger log = LoggerFactory.getLogger(InMemoryOffsetBackingStore.class);
    private volatile CdcSource cdcSource = null;
    private HashMap<byte[], byte[]> cache = new HashMap<>();

    public InMemoryOffsetBackingStore() {
    }

    public void configure(WorkerConfig config) {
        super.configure(config);
        String cdcSourceObjectId = (String) config.originals().get("cdc.source.object");
        cdcSource = ObjectKeeper.getCdcObject(cdcSourceObjectId);
    }

    public synchronized void start() {
        super.start();
        log.info("Started InMemoryOffsetBackingStore");
        this.load();
    }

    public synchronized void stop() {
        super.stop();
        log.info("Stopped InMemoryOffsetBackingStore");
    }

    private synchronized void load() {
//        Preferences prefs = Preferences.userNodeForPackage(InMemoryOffsetBackingStore.class);
//        cache = Util.stringToMap(prefs.get("mMapInstance", ""));
        Object cacheObj = cdcSource.currentState().get("cacheObj");
        cache = (HashMap<byte[], byte[]>) cacheObj;

        try {
            this.data = new HashMap<>();

            for (Object obj : cache.entrySet()) {
                Map.Entry<byte[], byte[]> mapEntry = (Map.Entry) obj;
                ByteBuffer key = mapEntry.getKey() != null ? ByteBuffer.wrap(mapEntry.getKey()) : null;
                ByteBuffer value = mapEntry.getValue() != null ? ByteBuffer.wrap(mapEntry.getValue()) : null;
                this.data.put(key, value);
            }
        } catch (Exception ex) {
            log.error("error loading the in-memory offsets.");
            log.error(ex.toString());
        }
    }

    protected synchronized void save() {
//        Preferences prefs = Preferences.userNodeForPackage(InMemoryOffsetBackingStore.class);
        try {
            for (Object o : this.data.entrySet()) {
                Map.Entry mapEntry = (Map.Entry) o;
                byte[] key = mapEntry.getKey() != null ? ((ByteBuffer) mapEntry.getKey()).array() : null;
                byte[] value = mapEntry.getValue() != null ? ((ByteBuffer) mapEntry.getValue()).array() : null;
                cache.put(key, value);
//                prefs.put("mMapInstance", Util.mapToString(cache));
            }

            Map<String, Object> currentState = new HashMap<>();
            currentState.put("cacheObj", cache);
            cdcSource.restoreState(currentState);
        } catch (Exception var7) {
            throw new ConnectException(var7);
        }
    }
}
