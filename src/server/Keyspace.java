package server;

import java.util.concurrent.ConcurrentHashMap;
import java.nio.charset.StandardCharsets;


public class Keyspace{

    private static final int NUM_STRIPES = 64;

    private final ConcurrentHashMap<String, RedisValue> data = new ConcurrentHashMap<>();

    private final Stripe[] stripes = new Stripe[NUM_STRIPES];

    public Keyspace(){
        for(int i = 0; i < NUM_STRIPES; i++){
            stripes[i] = new Stripe();
        }
    }

    private Stripe stripeFor(String key){
        return stripes[Math.floorMod(key.hashCode(), stripes.length)];
    }

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

    // ---- list commands ---------------------------------------------------

    // LPUSH key value -- prepend. Returns the new length.
    public long lpush(String key, byte[] value){
        Stripe s = stripeFor(key);
        s.lock.lock();
        try{
            long[] len = new long[1];
            data.compute(key, (k, curr) -> {
                RedisValue.ListValue l = asList(curr);        // creates if absent, throws if Str
                l.items().addFirst(value);
                len[0] = l.items().size();
                return l;
            });
            return len[0];
        }
        finally{
            s.lock.unlock();
        }
    }

    // RPUSH key value -- append. Same shape as lpush, addLast instead.
    public long rpush(String key, byte[] value){
        Stripe s = stripeFor(key);
        s.lock.lock();
        try{
            long[] len = new long[1];
            data.compute(key, (k, curr) -> {
                RedisValue.ListValue l = asList(curr);        // creates if absent, throws if Str
                l.items().addLast(value);
                len[0] = l.items().size();
                return l;
            });
            return len[0];
        }
        finally{
            s.lock.unlock();
        }
    }

    // LPOP key -- remove and return the head, or null if the key is absent.
    //
    // Returning null FROM THE LAMBDA deletes the entry. That is how an emptied
    // list stops existing, so EXISTS/KEYS do not report a phantom key.
    public byte[] lpop(String key){
        Stripe s = stripeFor(key);
        s.lock.lock();
        try{
            byte[][] popped = new byte[1][];
            data.compute(key, (k, curr) -> {
                if(curr == null){
                    return null;                              // absent stays absent
                }
                RedisValue.ListValue l = asList(curr);
                popped[0] = l.items().pollFirst();
                return l.items().isEmpty() ? null : l;
            });
            return popped[0];
        }
        finally{
            s.lock.unlock();
        }
    }

    // RPOP key -- same as lpop, pollLast instead.
    public byte[] rpop(String key){
        Stripe s = stripeFor(key);
        s.lock.lock();
        try{
            byte[][] popped = new byte[1][];
            data.compute(key, (k, curr) -> {
                if(curr == null){
                    return null;                              // absent stays absent
                }
                RedisValue.ListValue l = asList(curr);
                popped[0] = l.items().pollLast();
                return l.items().isEmpty() ? null : l;
            });
            return popped[0];
        }
        finally{
            s.lock.unlock();
        }
    }

    // LLEN key -- 0 if absent.
    public long llen(String key){
        Stripe s = stripeFor(key);
        s.lock.lock();
        try{
            long[] len = new long[1];
            data.computeIfPresent(key, (k, curr) -> {
                len[0] = asList(curr).items().size();
                return curr;
            });
            return len[0];
        }
        finally{
            s.lock.unlock();
        }
    }

    public Popped blpop(String key, long timeoutMs){
        Stripe stripe = stripeFor(key);
        stripe.lock.lock();
        try{
            
        }
        finally{
            stripe.lock.unlock();
        }
    }

    // Shared type dispatch for the list commands: absent -> a fresh empty list,
    // a list -> itself, anything else -> WRONGTYPE.
    private static RedisValue.ListValue asList(RedisValue curr){
        if(curr == null){
            return RedisValue.ListValue.empty();
        }
        if(curr instanceof RedisValue.ListValue l){
            return l;
        }
        throw new WrongTypeException();
    }
}
