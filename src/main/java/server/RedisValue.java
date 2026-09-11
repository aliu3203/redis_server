package server;

import java.util.ArrayDeque;
import java.util.Deque;
import java.nio.charset.StandardCharsets;


// A value stored in the keyspace. A key holds exactly one of these at a time;
// applying the wrong command to the wrong type is a -WRONGTYPE error.
public sealed interface RedisValue permits RedisValue.Str, RedisValue.ListValue{

    // SET / GET / INCR operate on this.
    record Str(byte[] bytes) implements RedisValue{
        
        public String parse(){
            return new String(bytes, StandardCharsets.ISO_8859_1);
        }
        public byte[] bytes(){
            return this.bytes;
        }

    }

    // LPUSH / RPUSH / LPOP / RPOP / BLPOP operate on this.
    //
    // ArrayDeque is NOT thread-safe, so every mutation must happen while
    // holding that key's lock -- i.e. inside a compute() lambda, or under
    // whatever per-key locking the list commands end up using.
    record ListValue(Deque<byte[]> items) implements RedisValue{

        static ListValue empty(){
            return new ListValue(new ArrayDeque<>());
        }
        public Deque<byte[]> items(){
            return this.items;
        }
        public boolean isEmpty(){
            return (items.size() == 0) ? true : false;
        }
    }
}
