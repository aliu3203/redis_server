package server;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

// BLPOP behaviour, driven through Keyspace and Dispatcher directly -- no sockets,
// no server. Each test controls "is the client still connected?" with a flag, so
// a disconnect can be simulated exactly when the test wants it.
//
//   javac -d out src/server/*.java test/server/*.java
//   java  -cp out server.BlpopTest
public class BlpopTest{

    private static int checks = 0;
    private static int failures = 0;

    private static final BooleanSupplier CONNECTED = () -> false;

    public static void main(String[] args){
        // --- basic behaviour ---
        run("immediateWhenListHasData", BlpopTest::immediateWhenListHasData);
        run("timeoutReturnsNullAndLeavesLine", BlpopTest::timeoutReturnsNullAndLeavesLine);
        run("lpushWakesBlockedClient", BlpopTest::lpushWakesBlockedClient);
        run("rpushWakesBlockedClient", BlpopTest::rpushWakesBlockedClient);
        run("fifoAcrossWaiters", BlpopTest::fifoAcrossWaiters);
        run("timedOutWaiterDoesNotSwallowPush", BlpopTest::timedOutWaiterDoesNotSwallowPush);

        // --- types ---
        run("wrongTypeFailsImmediately", BlpopTest::wrongTypeFailsImmediately);
        run("pushToKeyThatBecameStringIsRefused", BlpopTest::pushToKeyThatBecameStringIsRefused);

        // --- clients that leave ---
        run("disconnectedWaiterLeavesLine", BlpopTest::disconnectedWaiterLeavesLine);
        run("interruptedWaiterLeavesLine", BlpopTest::interruptedWaiterLeavesLine);
        run("valueForDisconnectedClientIsPutBack", BlpopTest::valueForDisconnectedClientIsPutBack);

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
    // tests: basic behaviour
    // ------------------------------------------------------------------

    // Data already there: BLPOP returns it at once, like LPOP, and never blocks.
    private static void immediateWhenListHasData() throws Exception{
        Keyspace ks = new Keyspace();
        ks.rpush("q", b("a"));
        long start = System.nanoTime();
        Popped p = ks.blpop("q", 5000, CONNECTED);
        long ms = elapsedMs(start);
        checkPopped("immediateWhenListHasData: returns the head", "q", "a", p);
        checkTrue("immediateWhenListHasData: did not wait (" + ms + " ms)", ms < 500);
        checkEquals("immediateWhenListHasData: list is now empty", 0L, ks.llen("q"));
    }

    // Nothing arrives: null after roughly the timeout, and the waiter takes
    // itself out of the line on the way out.
    private static void timeoutReturnsNullAndLeavesLine() throws Exception{
        Keyspace ks = new Keyspace();
        long start = System.nanoTime();
        Popped p = ks.blpop("q", 200, CONNECTED);
        long ms = elapsedMs(start);
        checkEquals("timeoutReturnsNullAndLeavesLine: returns null", null, p);
        checkTrue("timeoutReturnsNullAndLeavesLine: waited about the timeout (" + ms + " ms)", ms >= 150 && ms < 1500);
        checkEquals("timeoutReturnsNullAndLeavesLine: line is empty afterwards", 0, ks.waitersFor("q"));
    }

    private static void lpushWakesBlockedClient() throws Exception{
        pushWakesBlockedClient("lpushWakesBlockedClient", true);
    }

    private static void rpushWakesBlockedClient() throws Exception{
        pushWakesBlockedClient("rpushWakesBlockedClient", false);
    }

    // A push with a client waiting goes straight to that client: the push reports
    // length 1, the client wakes with the value, and the list never holds it.
    private static void pushWakesBlockedClient(String name, boolean left) throws Exception{
        Keyspace ks = new Keyspace();
        Blocked client = new Blocked(ks, "q", 5000, CONNECTED);
        checkTrue(name + ": client joined the line", awaitWaiters(ks, "q", 1));

        long start = System.nanoTime();
        long len = left ? ks.lpush("q", b("hello")) : ks.rpush("q", b("hello"));
        checkEquals(name + ": push replies length 1", 1L, len);
        checkTrue(name + ": client woke", client.finish(2000));
        long ms = elapsedMs(start);
        checkTrue(name + ": woke on the push, not the 5 s timeout (" + ms + " ms)", ms < 2000);
        checkPopped(name + ": client got the value", "q", "hello", client.result.get());
        checkEquals(name + ": value was never stored in the list", 0L, ks.llen("q"));
        checkEquals(name + ": line is empty", 0, ks.waitersFor("q"));
    }

    // The client that blocked first is served first.
    private static void fifoAcrossWaiters() throws Exception{
        Keyspace ks = new Keyspace();
        Blocked first = new Blocked(ks, "q", 5000, CONNECTED);
        checkTrue("fifoAcrossWaiters: first client joined", awaitWaiters(ks, "q", 1));
        Blocked second = new Blocked(ks, "q", 5000, CONNECTED);
        checkTrue("fifoAcrossWaiters: second client joined", awaitWaiters(ks, "q", 2));

        ks.rpush("q", b("one"));
        ks.rpush("q", b("two"));
        first.finish(2000);
        second.finish(2000);
        checkPopped("fifoAcrossWaiters: first to block gets the first value", "q", "one", first.result.get());
        checkPopped("fifoAcrossWaiters: second gets the second", "q", "two", second.result.get());
    }

    // A client that gave up must not take a later push away from one still waiting.
    private static void timedOutWaiterDoesNotSwallowPush() throws Exception{
        Keyspace ks = new Keyspace();
        Blocked quitter = new Blocked(ks, "q", 200, CONNECTED);
        checkTrue("timedOutWaiterDoesNotSwallowPush: short-timeout client joined", awaitWaiters(ks, "q", 1));
        Blocked stayer = new Blocked(ks, "q", 5000, CONNECTED);
        checkTrue("timedOutWaiterDoesNotSwallowPush: long-timeout client joined", awaitWaiters(ks, "q", 2));

        checkTrue("timedOutWaiterDoesNotSwallowPush: short-timeout client finished", quitter.finish(2000));
        checkEquals("timedOutWaiterDoesNotSwallowPush: and got nothing", null, quitter.result.get());

        ks.lpush("q", b("x"));
        stayer.finish(2000);
        checkPopped("timedOutWaiterDoesNotSwallowPush: push went to the client still waiting", "q", "x", stayer.result.get());
    }

    // ------------------------------------------------------------------
    // tests: types
    // ------------------------------------------------------------------

    // BLPOP on a string is an error straight away -- it must not block.
    private static void wrongTypeFailsImmediately() throws Exception{
        Keyspace ks = new Keyspace();
        ks.set("s", b("hi"));
        try{
            ks.blpop("s", 5000, CONNECTED);
            fail("wrongTypeFailsImmediately: expected WrongTypeException");
        }
        catch(WrongTypeException e){
            pass("wrongTypeFailsImmediately: WrongTypeException without blocking");
        }
        checkEquals("wrongTypeFailsImmediately: nobody joined the line", 0, ks.waitersFor("s"));
    }

    // A client is blocked on q, then q is SET to a string. A later LPUSH must be
    // refused with WRONGTYPE -- not hand its value to the waiting client.
    private static void pushToKeyThatBecameStringIsRefused() throws Exception{
        Keyspace ks = new Keyspace();
        Blocked client = new Blocked(ks, "q", 500, CONNECTED);
        checkTrue("pushToKeyThatBecameStringIsRefused: client joined the line", awaitWaiters(ks, "q", 1));

        ks.set("q", b("hi"));
        try{
            ks.lpush("q", b("x"));
            fail("pushToKeyThatBecameStringIsRefused: expected WrongTypeException");
        }
        catch(WrongTypeException e){
            pass("pushToKeyThatBecameStringIsRefused: LPUSH refused with WrongTypeException");
        }
        client.finish(2000);
        checkEquals("pushToKeyThatBecameStringIsRefused: waiting client was not handed the value", null, client.result.get());
    }

    // ------------------------------------------------------------------
    // tests: clients that leave
    // ------------------------------------------------------------------

    // The client disconnects while blocked with timeout 0, so no timeout would
    // ever end the wait. Between slices the waiter notices, leaves the line, and a
    // later push goes to the list instead of to a client that is gone.
    private static void disconnectedWaiterLeavesLine() throws Exception{
        Keyspace ks = new Keyspace();
        AtomicBoolean gone = new AtomicBoolean(false);
        Blocked client = new Blocked(ks, "q", 0, gone::get);
        checkTrue("disconnectedWaiterLeavesLine: client joined the line", awaitWaiters(ks, "q", 1));

        gone.set(true);
        checkTrue("disconnectedWaiterLeavesLine: noticed within a slice or two", client.finish(3000));
        checkEquals("disconnectedWaiterLeavesLine: returned null", null, client.result.get());
        checkEquals("disconnectedWaiterLeavesLine: left the line", 0, ks.waitersFor("q"));

        ks.lpush("q", b("v"));
        checkEquals("disconnectedWaiterLeavesLine: a later push stays in the list", 1L, ks.llen("q"));
    }

    // Interrupting a blocked client (what a shutdown would do) must take it out of
    // the line, or a later push would be handed to a thread that's no longer there.
    private static void interruptedWaiterLeavesLine() throws Exception{
        Keyspace ks = new Keyspace();
        Blocked client = new Blocked(ks, "q", 0, CONNECTED);
        checkTrue("interruptedWaiterLeavesLine: client joined the line", awaitWaiters(ks, "q", 1));

        client.thread.interrupt();
        checkTrue("interruptedWaiterLeavesLine: stopped waiting", client.finish(2000));
        checkTrue("interruptedWaiterLeavesLine: blpop threw InterruptedException",
                  client.error.get() instanceof InterruptedException);
        checkEquals("interruptedWaiterLeavesLine: left the line", 0, ks.waitersFor("q"));

        ks.lpush("q", b("v"));
        checkEquals("interruptedWaiterLeavesLine: a later push stays in the list", 1L, ks.llen("q"));
    }

    // The data-loss case: a push is handed to a client that disconnected moments
    // earlier. Dispatcher must notice before writing the reply, put the value
    // back on the list, and close the connection.
    private static void valueForDisconnectedClientIsPutBack() throws Exception{
        Keyspace ks = new Keyspace();
        Dispatcher d = new Dispatcher(ks);
        AtomicBoolean gone = new AtomicBoolean(false);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        AtomicReference<Throwable> error = new AtomicReference<>();
        Command blpop = new Command(new byte[][]{ b("BLPOP"), b("q"), b("0") });

        Thread connection = Thread.ofPlatform().daemon().start(() -> {
            try{
                d.dispatch(blpop, out, gone::get);
            }
            catch(Throwable t){
                error.set(t);
            }
        });
        checkTrue("valueForDisconnectedClientIsPutBack: client joined the line", awaitWaiters(ks, "q", 1));

        // Disconnect, then push within the same 1 s slice, so the push reaches the
        // waiter before blpop's own between-slices check would notice.
        gone.set(true);
        ks.lpush("q", b("v"));

        connection.join(2000);
        checkFalse("valueForDisconnectedClientIsPutBack: dispatch returned", connection.isAlive());
        checkTrue("valueForDisconnectedClientIsPutBack: connection closed with IOException, got " + error.get(),
                  error.get() instanceof IOException);
        checkEquals("valueForDisconnectedClientIsPutBack: no reply written to the dead client", 0, out.size());
        checkEquals("valueForDisconnectedClientIsPutBack: value put back on the list", 1L, ks.llen("q"));
    }

    // ------------------------------------------------------------------
    // helpers
    // ------------------------------------------------------------------

    // Runs keyspace.blpop on its own thread, the way a connection thread would.
    private static final class Blocked{
        final AtomicReference<Popped> result = new AtomicReference<>();
        final AtomicReference<Throwable> error = new AtomicReference<>();
        final Thread thread;

        Blocked(Keyspace ks, String key, long timeoutMs, BooleanSupplier clientGone){
            thread = Thread.ofPlatform().daemon().start(() -> {
                try{
                    result.set(ks.blpop(key, timeoutMs, clientGone));
                }
                catch(Throwable t){
                    error.set(t);
                }
            });
        }

        // Waits up to ms for blpop to return. True if it did.
        boolean finish(long ms) throws InterruptedException{
            thread.join(ms);
            return !thread.isAlive();
        }
    }

    // Waits until key's line holds n waiters, so a push is known to reach the
    // waiter rather than land on the list before the client has registered.
    private static boolean awaitWaiters(Keyspace ks, String key, int n) throws InterruptedException{
        long deadline = System.currentTimeMillis() + 2000;
        while(ks.waitersFor(key) != n){
            if(System.currentTimeMillis() > deadline){
                return false;
            }
            Thread.sleep(1);
        }
        return true;
    }

    private static byte[] b(String s){
        return s.getBytes(StandardCharsets.ISO_8859_1);
    }

    private static String str(byte[] bytes){
        return new String(bytes, StandardCharsets.ISO_8859_1);
    }

    private static long elapsedMs(long startNanos){
        return (System.nanoTime() - startNanos) / 1_000_000;
    }

    private static void checkPopped(String label, String key, String value, Popped p){
        if(p != null && p.key().equals(key) && str(p.value()).equals(value)){
            pass(label);
        }
        else{
            fail(label + ": expected (" + key + ", " + value + ") but was "
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
