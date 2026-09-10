package server;

import java.util.concurrent.ConcurrentHashMap;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.function.BooleanSupplier;


public class Keyspace{

    private static final int NUM_STRIPES = 64;

    // Longest single wait in BLPOP before re-checking that the client is still
    // connected.
    private static final long SLICE_MS = 1000;

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
            if(data.get(key) instanceof RedisValue.Str){
                throw new WrongTypeException();
            }

            if(serveWaiter(s, key, value) == true){
                return 1;
            }
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
            if(data.get(key) instanceof RedisValue.Str){
                throw new WrongTypeException();
            }

            if(serveWaiter(s, key, value) == true){
                return 1;
            }

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

    private byte[] popHeadLocked(String key){
        byte[][] popped = new byte[1][];
        data.compute(key, (k, curr) -> {
            if(curr == null) return null;
            RedisValue.ListValue l = asList(curr);
            popped[0] = l.items().pollFirst();
            return l.items().isEmpty() ? null : l;
        });
        return popped[0];
    }

    public Popped blpop(String key, long timeoutMs, BooleanSupplier clientGone) throws InterruptedException{

        Stripe stripe = stripeFor(key);
        Waiter w;
        stripe.lock.lock();
        try{
            byte[] v = popHeadLocked(key);
            if(v != null) return new Popped(key, v);
            w = new Waiter();
            stripe.waiters.computeIfAbsent(key, (k) -> new ArrayDeque<>()).addLast(w);
        }
        finally{
            stripe.lock.unlock();
        }

        // Wait in slices instead of one long wait, re-checking between slices
        // that the client is still connected. Otherwise a client that
        // disconnects during BLPOP ... 0 leaves its waiter in the line forever.
        long deadline = (timeoutMs == 0) ? Long.MAX_VALUE
                                         : System.currentTimeMillis() + timeoutMs;
        while(true){
            long remaining = deadline - System.currentTimeMillis();
            if(remaining <= 0){
                break;                                     // timed out
            }
            Popped p;
            try{
                p = w.await(Math.min(remaining, SLICE_MS));
            }
            catch(InterruptedException e){
                abandon(stripe, key, w);
                throw e;
            }
            if(p != null){
                return p;
            }
            if(clientGone.getAsBoolean()){
                abandon(stripe, key, w);
                return null;
            }
        }

        // Timed out. If a pusher claimed w just before the deadline, its value is
        // already committed to us, so collect it rather than reply *-1.
        if(w.cancel()){
            removeFromLine(stripe, key, w);
            return null;
        }
        return takeUninterruptibly(w);
    }

    // The waiting client is leaving (interrupted or disconnected) and won't read
    // a reply. If nobody has claimed w, take it out of the line. If a pusher
    // already claimed it, that value must not be dropped: collect it and push it
    // back, which hands it to the next waiter or returns it to the list.
    private void abandon(Stripe stripe, String key, Waiter w){
        if(w.cancel()){
            removeFromLine(stripe, key, w);
            return;
        }
        Popped claimed = takeUninterruptibly(w);
        lpush(claimed.key(), claimed.value());
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

    private void removeFromLine(Stripe stripe, String key, Waiter w){
        stripe.lock.lock();
        try{
            Deque<Waiter> line = stripe.waiters.get(key);

            if(line != null){
                line.remove(w);
                if(line.isEmpty()){
                    stripe.waiters.remove(key);
                }
            }
        }
        finally{
            stripe.lock.unlock();
        }
    }
    
    private Popped takeUninterruptibly(Waiter w){
        boolean interrupted = false;
        try{
            while(true){
                try{
                    return w.await(0);
                } catch(InterruptedException e){
                    interrupted = true;          // remember it, keep waiting
                }
            }
        } finally{
            if(interrupted) Thread.currentThread().interrupt();   // restore on the way out
        }
    }

    private boolean serveWaiter(Stripe s, String key, byte[] value){
        Deque<Waiter> line = s.waiters.get(key);
        if(line == null) return false;
        while(!line.isEmpty()){
            Waiter w = line.pollFirst();
            if(w.tryDeliver(key, value)){
                if(line.isEmpty()){
                    s.waiters.remove(key);
                }
                return true;
            }
        }
        // remaining dead waiters
        s.waiters.remove(key);

        // no waiters served
        return false;

    }
}
