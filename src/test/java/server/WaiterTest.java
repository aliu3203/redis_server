package server;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import static server.TestAsserts.assertPopped;

// Unit tests for Waiter on its own -- no Keyspace, no server.
//
//   mvn test -Dtest=WaiterTest
class WaiterTest{

    private static final byte[] V = bytes("job");

    // A value delivered before anyone waits is still there when they do.
    @Test
    void deliverThenAwait() throws Exception{
        Waiter w = new Waiter();
        assertTrue(w.tryDeliver("q", V), "tryDeliver succeeds");
        assertPopped("q", V, w.await(100));
    }

    // The normal case: one thread waits, another delivers, and the waiter wakes
    // on the delivery rather than at its timeout.
    @Test
    void deliverWakesWaitingThread() throws Exception{
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
        assertTrue(w.tryDeliver("q", V), "tryDeliver succeeds");
        t.join(2000);
        long ms = elapsedMs(start);
        assertFalse(t.isAlive(), "waiter finished");
        assertTrue(ms < 1000, "woke on delivery, not the 5 s timeout (" + ms + " ms)");
        assertPopped("q", V, got.get());
    }

    // await(0) means "wait forever" (BLPOP key 0), not "don't wait".
    @Test
    void awaitZeroWaitsUntilDelivered() throws Exception{
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
        assertTrue(t.isAlive(), "still waiting after 300 ms");
        w.tryDeliver("q", V);
        t.join(2000);
        assertPopped("q", V, got.get());
    }

    // Nobody delivers: await gives up, cancel succeeds, and a late pusher is
    // refused -- so it keeps its value instead of dropping it into a dead slot.
    @Test
    void timeoutThenCancel() throws Exception{
        Waiter w = new Waiter();
        assertNull(w.await(100), "await times out");
        assertTrue(w.cancel(), "cancel succeeds");
        assertFalse(w.tryDeliver("q", V), "late tryDeliver is refused");
    }

    // The race that would lose data, forced into a fixed order: the waiter times
    // out, a pusher claims it before cancel() runs, so cancel() fails -- and the
    // value must still be collectable. (Collected with a 1 s bound rather than
    // await(0), so a regression fails this test instead of hanging it.)
    @Test
    void deliverAfterTimeoutBeforeCancel() throws Exception{
        Waiter w = new Waiter();
        assertNull(w.await(50), "await times out");
        assertTrue(w.tryDeliver("q", V), "pusher claims it");
        assertFalse(w.cancel(), "cancel fails");
        assertPopped("q", V, w.await(1000));
    }

    // Two pushers reaching the same waiter: exactly one delivers, and the other
    // is told so and keeps its value.
    @Test
    void onlyFirstDeliveryWins() throws Exception{
        Waiter w = new Waiter();
        assertTrue(w.tryDeliver("q", V), "first delivery succeeds");
        assertFalse(w.tryDeliver("q", bytes("other")), "second is refused");
        assertPopped("q", V, w.await(100));
    }

    // tryDeliver and cancel released at the same instant, many times over. The
    // compareAndSet must pick exactly one winner every round: if both "win", the
    // pusher thinks it delivered to a client that has already left, and the
    // value is lost.
    @Test
    void deliverVsCancelRace() throws Exception{
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

        // Copies, because lambdas can only capture effectively-final variables.
        final int both = bothWon;
        final int neither = neitherWon;
        final int missing = deliveredButMissing;
        assertAll(
            () -> assertEquals(0, both, "rounds where both won (of " + rounds + ")"),
            () -> assertEquals(0, neither, "rounds where neither won"),
            () -> assertEquals(0, missing, "delivered but not collectable")
        );
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

    private static long elapsedMs(long startNanos){
        return (System.nanoTime() - startNanos) / 1_000_000;
    }
}
