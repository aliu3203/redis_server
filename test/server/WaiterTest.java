package server;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

// Unit tests for Waiter on its own -- no Keyspace, no server.
//
//   javac -d out src/server/*.java test/server/*.java
//   java  -cp out server.WaiterTest
public class WaiterTest{

    private static int checks = 0;
    private static int failures = 0;

    private static final byte[] V = bytes("job");

    public static void main(String[] args){
        run("deliverThenAwait", WaiterTest::deliverThenAwait);
        run("deliverWakesWaitingThread", WaiterTest::deliverWakesWaitingThread);
        run("awaitZeroWaitsUntilDelivered", WaiterTest::awaitZeroWaitsUntilDelivered);
        run("timeoutThenCancel", WaiterTest::timeoutThenCancel);
        run("deliverAfterTimeoutBeforeCancel", WaiterTest::deliverAfterTimeoutBeforeCancel);
        run("onlyFirstDeliveryWins", WaiterTest::onlyFirstDeliveryWins);
        run("deliverVsCancelRace", WaiterTest::deliverVsCancelRace);

        System.out.println();
        System.out.println(checks + " checks, " + failures + " failed");
        if(failures > 0){
            System.exit(1);
        }
    }

    private interface Testable{
        void run() throws Exception;
    }

    // Runs one test in isolation. An unexpected exception fails that test and
    // the run continues, so a single crash cannot hide the rest.
    private static void run(String name, Testable t){
        try{
            t.run();
        }
        catch(Throwable e){
            fail(name + ": threw " + e);
        }
    }

    // ------------------------------------------------------------------
    // tests
    // ------------------------------------------------------------------

    // A value delivered before anyone waits is still there when they do.
    private static void deliverThenAwait() throws Exception{
        Waiter w = new Waiter();
        checkTrue("deliverThenAwait: tryDeliver succeeds", w.tryDeliver("q", V));
        checkPopped("deliverThenAwait: await returns it", "q", V, w.await(100));
    }

    // The normal case: one thread waits, another delivers, and the waiter wakes
    // on the delivery rather than at its timeout.
    private static void deliverWakesWaitingThread() throws Exception{
        Waiter w = new Waiter();
        AtomicReference<Popped> got = new AtomicReference<>();
        Thread t = Thread.ofPlatform().daemon().start(() -> {
            try{
                got.set(w.await(5000));
            }
            catch(InterruptedException ignored){
            }
        });
        Thread.sleep(100);
        long start = System.nanoTime();
        checkTrue("deliverWakesWaitingThread: tryDeliver succeeds", w.tryDeliver("q", V));
        t.join(2000);
        long ms = elapsedMs(start);
        checkFalse("deliverWakesWaitingThread: waiter finished", t.isAlive());
        checkTrue("deliverWakesWaitingThread: woke on delivery, not the 5 s timeout (" + ms + " ms)", ms < 1000);
        checkPopped("deliverWakesWaitingThread: waiter got the value", "q", V, got.get());
    }

    // await(0) means "wait forever" (BLPOP key 0), not "don't wait".
    private static void awaitZeroWaitsUntilDelivered() throws Exception{
        Waiter w = new Waiter();
        AtomicReference<Popped> got = new AtomicReference<>();
        Thread t = Thread.ofPlatform().daemon().start(() -> {
            try{
                got.set(w.await(0));
            }
            catch(InterruptedException ignored){
            }
        });
        Thread.sleep(300);
        checkTrue("awaitZeroWaitsUntilDelivered: still waiting after 300 ms", t.isAlive());
        w.tryDeliver("q", V);
        t.join(2000);
        checkPopped("awaitZeroWaitsUntilDelivered: returns the value once delivered", "q", V, got.get());
    }

    // Nobody delivers: await gives up, cancel succeeds, and a late pusher is
    // refused -- so it keeps its value instead of dropping it into a dead slot.
    private static void timeoutThenCancel() throws Exception{
        Waiter w = new Waiter();
        checkEquals("timeoutThenCancel: await times out", null, w.await(100));
        checkTrue("timeoutThenCancel: cancel succeeds", w.cancel());
        checkFalse("timeoutThenCancel: late tryDeliver is refused", w.tryDeliver("q", V));
    }

    // The race that would lose data, forced into a fixed order: the waiter times
    // out, a pusher claims it before cancel() runs, so cancel() fails -- and the
    // value must still be collectable. (Collected with a 1 s bound rather than
    // await(0), so a regression fails this test instead of hanging it.)
    private static void deliverAfterTimeoutBeforeCancel() throws Exception{
        Waiter w = new Waiter();
        checkEquals("deliverAfterTimeoutBeforeCancel: await times out", null, w.await(50));
        checkTrue("deliverAfterTimeoutBeforeCancel: pusher claims it", w.tryDeliver("q", V));
        checkFalse("deliverAfterTimeoutBeforeCancel: cancel fails", w.cancel());
        checkPopped("deliverAfterTimeoutBeforeCancel: value is not lost", "q", V, w.await(1000));
    }

    // Two pushers reaching the same waiter: exactly one delivers, and the other
    // is told so and keeps its value.
    private static void onlyFirstDeliveryWins() throws Exception{
        Waiter w = new Waiter();
        checkTrue("onlyFirstDeliveryWins: first delivery succeeds", w.tryDeliver("q", V));
        checkFalse("onlyFirstDeliveryWins: second is refused", w.tryDeliver("q", bytes("other")));
        checkPopped("onlyFirstDeliveryWins: waiter gets the first value", "q", V, w.await(100));
    }

    // tryDeliver and cancel released at the same instant, many times over. The
    // compareAndSet must pick exactly one winner every round: if both "win", the
    // pusher thinks it delivered to a client that has already left, and the
    // value is lost.
    private static void deliverVsCancelRace() throws Exception{
        final int rounds = 5000;
        int bothWon = 0;
        int neitherWon = 0;
        int deliveredButMissing = 0;
        for(int i = 0; i < rounds; i++){
            Waiter w = new Waiter();
            CountDownLatch go = new CountDownLatch(1);
            AtomicBoolean delivered = new AtomicBoolean();
            AtomicBoolean cancelled = new AtomicBoolean();
            Thread pusher = Thread.ofVirtual().start(() -> {
                waitFor(go);
                delivered.set(w.tryDeliver("q", V));
            });
            Thread leaver = Thread.ofVirtual().start(() -> {
                waitFor(go);
                cancelled.set(w.cancel());
            });
            go.countDown();
            pusher.join();
            leaver.join();
            if(delivered.get() && cancelled.get()){
                bothWon++;
            }
            if(!delivered.get() && !cancelled.get()){
                neitherWon++;
            }
            if(delivered.get() && w.await(1000) == null){
                deliveredButMissing++;
            }
        }
        checkEquals("deliverVsCancelRace: rounds where both won (of " + rounds + ")", 0, bothWon);
        checkEquals("deliverVsCancelRace: rounds where neither won", 0, neitherWon);
        checkEquals("deliverVsCancelRace: delivered but not collectable", 0, deliveredButMissing);
    }

    // ------------------------------------------------------------------
    // helpers
    // ------------------------------------------------------------------

    private static void waitFor(CountDownLatch latch){
        try{
            latch.await();
        }
        catch(InterruptedException ignored){
        }
    }

    private static byte[] bytes(String s){
        return s.getBytes(StandardCharsets.ISO_8859_1);
    }

    private static String str(byte[] b){
        return new String(b, StandardCharsets.ISO_8859_1);
    }

    private static long elapsedMs(long startNanos){
        return (System.nanoTime() - startNanos) / 1_000_000;
    }

    private static void checkPopped(String label, String key, byte[] value, Popped p){
        if(p != null && p.key().equals(key) && Arrays.equals(p.value(), value)){
            pass(label);
        }
        else{
            fail(label + ": expected (" + key + ", " + str(value) + ") but was "
                 + (p == null ? "null" : "(" + p.key() + ", " + str(p.value()) + ")"));
        }
    }

    private static void checkEquals(String label, Object expected, Object actual){
        boolean ok = (expected == null) ? (actual == null) : expected.equals(actual);
        if(ok){
            pass(label);
        }
        else{
            fail(label + ": expected <" + expected + "> but was <" + actual + ">");
        }
    }

    private static void checkTrue(String label, boolean condition){
        if(condition){
            pass(label);
        }
        else{
            fail(label);
        }
    }

    private static void checkFalse(String label, boolean condition){
        checkTrue(label, !condition);
    }

    private static void pass(String label){
        checks++;
        System.out.println("  ok   " + label);
    }

    private static void fail(String label){
        checks++;
        failures++;
        System.out.println("  FAIL " + label);
    }
}
