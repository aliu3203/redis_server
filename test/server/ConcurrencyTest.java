package server;

import java.nio.charset.StandardCharsets;

public class ConcurrencyTest{

    private static final int THREADS = 50;
    private static final int NUM_OPS = 1000;
    private static final int RUNS = 5;
    private static final int TARGET = THREADS*NUM_OPS;

    private static final long TIMEOUT_MS = 5000;

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
    private static boolean report(int run, String label, long got, int stuck){
        if(stuck > 0){
            System.out.printf("   run %d: HUNG -- %d thread(s) still running after %ds%n",
                              run, stuck, TIMEOUT_MS / 1000);
            return false;
        }
        boolean pass = (got == TARGET);
        System.out.printf("   run %d: %-7s = %,7d / %,d   %s%n",
                          run, label, got, TARGET, pass ? "ok" : "LOST " + (TARGET - got));
        return pass;
    }

    public static boolean runA(int run) throws InterruptedException{
        final Keyspace ks = new Keyspace();
        Thread[] ts = new Thread[THREADS];

        for(int i = 0; i < THREADS; i++){
            final int id = i;
            ts[i] = Thread.ofPlatform().daemon().start(()->{
                for(int k = 0; k < NUM_OPS; k++){
                    ks.incr("counter");
                }
            });
        }

        long deadline = System.currentTimeMillis() + TIMEOUT_MS;
        for(Thread t : ts){
            long remaining = deadline-System.currentTimeMillis();
            if(remaining <= 0){
                break;
            }
            t.join(remaining);
        }
        int stuck = 0;
        for(Thread t: ts){
            if(t.isAlive()){
                stuck++;
            }
        }

        byte[] raw = ks.get("counter");
        long got = (raw == null) ? 0 : Long.parseLong(new String(raw, StandardCharsets.ISO_8859_1));
        return report(run, "counter", got, stuck);
    }
    public static boolean runB(int run) throws InterruptedException{
        Keyspace ks = new Keyspace();

        // TODO: 50 threads x NUM_OPS set("k<id>_<k>", VALUE), distinct keys.
        // Then count non-null gets single-threaded and:
        //     return report(run, "keys", found, stuck);
        return true;
    }
}
