package server;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Timeout;
import static org.junit.jupiter.api.Assertions.*;

// Concurrency stress tests for Keyspace. Races are probabilistic, so each case
// repeats: one clean run proves little, five in a row is convincing.
//
// The SEPARATE_THREAD timeout matters here: a corrupted HashMap can leave a
// thread spinning forever without ever blocking, and only a timeout that runs
// the test on its own thread can walk away from it instead of hanging the build.
//
//   mvn test -Dtest=ConcurrencyTest
@Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class ConcurrencyTest{

    private static final int REPEATS = 5;

    private static final int THREADS = 50;
    private static final int NUM_OPS = 1000;
    private static final int TARGET = THREADS * NUM_OPS;

    // How long a run waits for its threads, in total (not per thread).
    private static final long TIMEOUT_MS = 5000;

    // Case D: consumers BLPOP while producers RPUSH, on one key.
    private static final int D_THREADS = 20;             // per side
    private static final int D_OPS = 1000;               // per thread
    private static final int D_TARGET = D_THREADS * D_OPS;
    private static final long D_BLPOP_TIMEOUT_MS = 2000;

    private static final byte[] VALUE = {'v'};

    // A: ONE key, many increments. Exposes the read-modify-write race inside
    // incr(). ConcurrentHashMap alone does NOT fix it -- the race is across the
    // get and the put, not inside either one. compute() does.
    @RepeatedTest(REPEATS)
    void incrOnOneKeyIsAtomic() throws Exception{
        Keyspace ks = new Keyspace();
        AtomicInteger errors = new AtomicInteger();
        Thread[] ts = new Thread[THREADS];
        for(int i = 0; i < THREADS; i++){
            ts[i] = start(errors, () -> {
                for(int k = 0; k < NUM_OPS; k++){
                    ks.incr("counter");
                }
            });
        }

        assertAllFinished(ts);
        assertEquals(0, errors.get(), "threads that threw");
        RedisValue.Str counter = assertInstanceOf(RedisValue.Str.class, ks.get("counter"), "counter holds a string");
        assertEquals(TARGET, Long.parseLong(counter.parse()), "counter");
    }

    // B: MANY distinct keys, forcing concurrent resizes of the underlying table.
    // Every key is written by exactly one thread, so nothing races logically: a
    // missing key was destroyed by the map itself. ConcurrentHashMap DOES fix
    // this. A single-key test never resizes, so it cannot show it.
    @RepeatedTest(REPEATS)
    void setOnDistinctKeysLosesNothing() throws Exception{
        Keyspace ks = new Keyspace();
        AtomicInteger errors = new AtomicInteger();
        Thread[] ts = new Thread[THREADS];
        for(int i = 0; i < THREADS; i++){
            final int id = i;
            ts[i] = start(errors, () -> {
                for(int k = 0; k < NUM_OPS; k++){
                    ks.set("k" + id + "_" + k, VALUE);
                }
            });
        }

        assertAllFinished(ts);
        assertEquals(0, errors.get(), "threads that threw");

        // Every writer has finished, so this pass is race-free: a key missing
        // here was genuinely lost, not merely not-yet-written.
        long found = 0;
        for(int id = 0; id < THREADS; id++){
            for(int k = 0; k < NUM_OPS; k++){
                if(ks.get("k" + id + "_" + k) != null){
                    found++;
                }
            }
        }
        assertEquals(TARGET, found, "keys present");
    }

    // C: ONE list, many pushes. Every change to the ArrayDeque must happen under
    // the key's lock; one that escapes it tears the deque apart (it can even end
    // up reporting a negative size).
    @RepeatedTest(REPEATS)
    void lpushOnOneKeyLosesNothing() throws Exception{
        Keyspace ks = new Keyspace();
        AtomicInteger errors = new AtomicInteger();
        Thread[] ts = new Thread[THREADS];
        for(int i = 0; i < THREADS; i++){
            ts[i] = start(errors, () -> {
                for(int k = 0; k < NUM_OPS; k++){
                    ks.lpush("list", VALUE);
                }
            });
        }

        assertAllFinished(ts);
        assertEquals(0, errors.get(), "threads that threw");
        assertEquals(TARGET, ks.llen("list"), "list length");
    }

    // D: blocking handoff under contention. Consumers BLPOP exactly as many times
    // as producers push, so every value must reach exactly one consumer. A value
    // received twice was duplicated; a value never received was lost, and some
    // consumer times out waiting for it; anything left in the list is a value a
    // consumer gave up on while it was there.
    @RepeatedTest(REPEATS)
    void blpopDeliversEveryValueExactlyOnce() throws Exception{
        Keyspace ks = new Keyspace();
        AtomicInteger errors = new AtomicInteger();
        AtomicInteger timeouts = new AtomicInteger();
        ConcurrentHashMap<String, Integer> received = new ConcurrentHashMap<>();
        Thread[] ts = new Thread[D_THREADS * 2];

        for(int i = 0; i < D_THREADS; i++){
            ts[i] = start(errors, () -> {
                for(int k = 0; k < D_OPS; k++){
                    Popped p = ks.blpop("jobs", D_BLPOP_TIMEOUT_MS, () -> false);
                    if(p == null){
                        timeouts.incrementAndGet();
                    }
                    else{
                        received.merge(new String(p.value(), StandardCharsets.ISO_8859_1), 1, Integer::sum);
                    }
                }
            });
        }
        for(int i = 0; i < D_THREADS; i++){
            final int id = i;
            ts[D_THREADS + i] = start(errors, () -> {
                for(int k = 0; k < D_OPS; k++){
                    ks.rpush("jobs", ("p" + id + "_" + k).getBytes(StandardCharsets.ISO_8859_1));
                }
            });
        }

        assertAllFinished(ts);
        long duplicated = received.values().stream().filter(n -> n > 1).count();
        long lost = D_TARGET - received.size();
        long left = ks.llen("jobs");
        assertAll(
            () -> assertEquals(0, errors.get(), "threads that threw"),
            () -> assertEquals(0L, lost, "values never delivered"),
            () -> assertEquals(0L, duplicated, "values delivered more than once"),
            () -> assertEquals(0L, left, "values left in the list"),
            () -> assertEquals(0, timeouts.get(), "BLPOPs that timed out")
        );
    }

    // ------------------------------------------------------------------
    // helpers
    // ------------------------------------------------------------------

    private interface Body{
        void run() throws Exception;
    }

    // Starts a daemon platform thread: real parallelism across cores makes races
    // far more likely to show. A thread that throws -- a corrupted map can throw
    // from inside put or get -- dies there and loses its remaining work, so it is
    // counted, and shows up as a cause rather than as mysterious loss.
    private static Thread start(AtomicInteger errors, Body body){
        return Thread.ofPlatform().daemon().start(() -> {
            try{
                body.run();
            }
            catch(Exception e){
                errors.incrementAndGet();
            }
        });
    }

    // Waits for every thread, bounded by TIMEOUT_MS in total, and fails if any
    // are still running: their results can't be trusted, since they're still
    // changing the map. Checked before anything else for that reason.
    private static void assertAllFinished(Thread[] ts) throws InterruptedException{
        long deadline = System.currentTimeMillis() + TIMEOUT_MS;
        for(Thread t : ts){
            long remaining = deadline - System.currentTimeMillis();
            if(remaining <= 0){
                break;                       // join(0) would wait forever
            }
            t.join(remaining);
        }
        int stuck = 0;
        for(Thread t : ts){
            if(t.isAlive()){
                stuck++;
            }
        }
        assertEquals(0, stuck, "threads still running after " + TIMEOUT_MS / 1000 + " s -- hung?");
    }
}
