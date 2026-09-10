package server;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;

public class ConcurrencyTest{

    private static final int THREADS = 50;
    private static final int NUM_OPS = 1000;
    private static final int RUNS = 5;
    private static final int TARGET = THREADS*NUM_OPS;

    private static final long TIMEOUT_MS = 5000;

    private static final byte[] VALUE = {'v'};

    public static void main(String[] args) throws InterruptedException{
        boolean ok = true;

        System.out.println("A: " + THREADS + " threads x " + NUM_OPS + " INCR on ONE key");
        for(int i = 1; i <= RUNS; i++){
            ok &= runA(i);
        }

        System.out.println();
        System.out.println("B: " + THREADS + " threads x " + NUM_OPS + " SET on DISTINCT keys");
        for(int i = 1; i <= RUNS; i++){
            ok &= runB(i);
        }

        System.out.println();
        System.out.println(ok ? "PASS" : "FAIL");
        if(!ok){
            System.exit(1);
        }
    }

    // Prints one result line and returns whether the run passed.
    //
    // A run with stuck threads has no trustworthy value to read -- those threads
    // are still mutating the map -- so it is reported as HUNG rather than as a
    // count that looks like a measurement.
    private static boolean report(int run, String label, long got, int stuck, int errors){
        if(stuck > 0){
            System.out.printf("   run %d: HUNG -- %d thread(s) still running after %ds%n",
                              run, stuck, TIMEOUT_MS / 1000);
            return false;
        }
        boolean pass = (got == TARGET) && (errors == 0);
        String note = pass ? "ok"
                           : "LOST " + (TARGET - got)
                             + (errors > 0 ? ", " + errors + " thread(s) threw" : "");
        System.out.printf("   run %d: %-7s = %,7d / %,d   %s%n",
                          run, label, got, TARGET, note);
        return pass;
    }

    // Waits for every thread, bounded by TIMEOUT_MS in total (not per thread).
    // Returns how many are still running once the budget is spent -- a thread
    // spinning inside a corrupted HashMap never terminates on its own.
    private static int awaitAll(Thread[] ts) throws InterruptedException{
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
        return stuck;
    }

    public static boolean runA(int run) throws InterruptedException{
        final Keyspace ks = new Keyspace();
        Thread[] ts = new Thread[THREADS];
        AtomicInteger errors = new AtomicInteger();

        for(int i = 0; i < THREADS; i++){
            ts[i] = Thread.ofPlatform().daemon().start(()->{
                try{
                    for(int k = 0; k < NUM_OPS; k++){
                        ks.incr("counter");
                    }
                }
                catch(RuntimeException e){
                    // A corrupted map can throw from inside put/get. The thread
                    // dies here and loses its remaining increments -- tally it so
                    // that shows up as a cause rather than as mysterious loss.
                    errors.incrementAndGet();
                }
            });
        }

        int stuck = awaitAll(ts);
        if(stuck > 0){
            return report(run, "counter", 0, stuck, errors.get());
        }

        long got;
        try{
            byte[] raw = ks.get("counter");
            got = (raw == null) ? 0 : Long.parseLong(new String(raw, StandardCharsets.ISO_8859_1));
        }
        catch(RuntimeException e){
            System.out.printf("   run %d: read failed -- %s%n", run, e.getClass().getSimpleName());
            return false;
        }
        return report(run, "counter", got, stuck, errors.get());
    }
    public static boolean runB(int run) throws InterruptedException{
        final Keyspace ks = new Keyspace();
        Thread[] ts = new Thread[THREADS];
        AtomicInteger errors = new AtomicInteger();

        for(int i = 0; i < THREADS; i++){
            final int id = i;
            ts[i] = Thread.ofPlatform().daemon().start(() -> {
                try{
                    for(int k = 0; k < NUM_OPS; k++){
                        ks.set("k"+id+"_" + k, VALUE);
                    }
                }
                catch(RuntimeException e){
                    // A corrupted map throws from inside put(). The thread dies
                    // here and loses its remaining writes -- tally it so that
                    // shows up as a cause rather than as mysterious loss.
                    errors.incrementAndGet();
                }
            });
        }

        int stuck = awaitAll(ts);

        if(stuck > 0){
            return report(run, "keys", 0, stuck, errors.get());
        }

        // Every writer has finished, so this pass is race-free: a key that is
        // missing here was genuinely lost by the map, not merely not-yet-written.
        long found = 0;
        try{
            for(int id = 0; id < THREADS; id++){
                for(int k = 0; k < NUM_OPS; k++){
                    if(ks.get("k"+id+"_" + k) != null){
                        found++;
                    }
                }
            }
        }
        catch(RuntimeException e){
            // A half-treeified bin can throw during traversal too, so even the
            // read-back is not safe against a map this damaged.
            System.out.printf("   run %d: count failed -- %s%n", run, e.getClass().getSimpleName());
            return false;
        }
        return report(run, "keys", found, stuck, errors.get());
    }
}
