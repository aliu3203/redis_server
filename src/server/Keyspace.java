package server;

import java.util.Map;
import java.util.HashMap;
import java.nio.charset.StandardCharsets;

public class Keyspace{
    private final Map<String, byte[]> data = new HashMap<>();

    public byte[] get(String key){
        return data.get(key);
    }
    public void set(String key, byte[] value){
        data.put(key, value);
    }

    // No handling of LONG.MAX_VALUE
    public long incr(String key){
        byte[] curr = data.get(key);
        long n = (curr == null) ? 0 : Long.parseLong(new String(cur, StandardCharsets.ISO_8859_1));
        n += 1;
        data.put(key, Long.toString(n).getBytes(StandardCharsets.ISO_8859_1));
        return n;
    }
}
