package server;

import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.nio.charset.StandardCharsets;

public class Keyspace{
    private final ConcurrentHashMap<String, byte[]> data = new ConcurrentHashMap<>();

    public byte[] get(String key){
        return data.get(key);
    }
    public void set(String key, byte[] value){
        data.put(key, value);
    }

    public long incr(String key){
        byte[] updated = data.compute(key, (k, curr) -> {
            long n = (curr == null) ? 0 : Long.parseLong(new String(curr, StandardCharsets.ISO_8859_1));
            n = Math.addExact(n, 1);
            return Long.toString(n).getBytes(StandardCharsets.ISO_8859_1);
        });
        return Long.parseLong(new String(updated, StandardCharsets.ISO_8859_1));
    }
}
