package server;

public class ConcurrencyTest{

    private static final int THREADS = 50;
    private static final int NUM_OPS = 1000;
    private static final int RUNS = 5;
    private static final int TARGET = THREADS*NUM_OPS;

    private static final long TIMEOUT_MS = 5000;

    public static void main(String[] args){
        for(int i = 0; i < RUNS; i++){
            runA();
        }
        for(int i = 0; i < RUNS; i++){
            runB();
        }
    }

    public static bool runA(){
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

        long deadline = System.currentTimeMillis() + TIMEOUT_MS
        for(Thread t : ts){
            long remaining = deadline-System.currentTimeMillis();
            if(remaining <= 0){
                break;
            }
            t.join(remaining);
        }
        int stuck;
        for(Thread t: ts){
            if(t.isAlive()){
                stuck++;
            }
        }
        return true;
    }
    public static void runB(){
        Keyspace ks = new Keyspace();

    }
}
