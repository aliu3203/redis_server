package server;

import java.util.concurrent.ConcurrentHashMap;
import java.nio.charset.StandardCharsets;


public class Keyspace{
    private final ConcurrentHashMap<String, RedisValue> data = new ConcurrentHashMap<>();

    public RedisValue get(String key){
        return data.get(key);
    }
    public void set(String key, byte[] value){
        data.put(key, new RedisValue.Str(value));
    }

    public long incr(String key){
        RedisValue updated = data.compute(key, (k, curr) -> {
            long n;
            if(curr == null){
                n = 0;
            }
            else if(curr instanceof RedisValue.Str s){
                n = Long.parseLong(s.parse());
            }
            else{
                throw new WrongTypeException();
            }
            n = Math.addExact(n, 1);
            return new RedisValue.Str(Long.toString(n).getBytes(StandardCharsets.ISO_8859_1));
        });
        return Long.parseLong(((RedisValue.Str)updated).parse());
    }
}
