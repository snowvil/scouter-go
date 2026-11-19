package scouter.client.preferences;

import java.util.concurrent.ConcurrentHashMap;

/** Headless UI preference adapter only. No network, server registry or login behavior. */
public final class PManager {
    private static final PManager INSTANCE = new PManager();
    private final ConcurrentHashMap<String, String> values = new ConcurrentHashMap<>();

    private PManager() {}

    public static PManager getInstance() {
        return INSTANCE;
    }

    public String getString(String key) {
        return values.getOrDefault(key, "");
    }

    public void setValue(String key, String value) {
        values.put(key, value);
    }
}
