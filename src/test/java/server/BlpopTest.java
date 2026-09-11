package server;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import static org.junit.jupiter.api.Assertions.*;
import static server.TestAsserts.assertPopped;

// BLPOP behaviour, driven through Keyspace and Dispatcher directly -- no sockets,
// no server. Each test controls "is the client still connected?" with a flag, so
// a disconnect can be simulated exactly when the test wants it.
//
// Every wait below is already bounded. The class-level timeout is insurance: a
// future bug that blocks forever fails one test instead of hanging the build.
//
//   mvn test -Dtest=BlpopTest
@Timeout(10)
class BlpopTest{

    private static final BooleanSupplier CONNECTED = () -> false;

    // ------------------------------------------------------------------
    // basic behaviour
    // ------------------------------------------------------------------

    // Data already there: BLPOP returns it at once, like LPOP, and never blocks.
    @Test
    void immediateWhenListHasData() throws Exception{
        Keyspace ks = new Keyspace();
        ks.rpush("q", b("a"));
        long start = System.nanoTime();
        Popped p = ks.blpop("q", 5000, CONNECTED);
        long ms = elapsedMs(start);
        assertPopped("q", "a", p);
        assertTrue(ms < 500, "did not wait (" + ms + " ms)");
        assertEquals(0L, ks.llen("q"), "list is now empty");
    }

    // Nothing arrives: null after roughly the timeout, and the waiter takes
    // itself out of the line on the way out.
    @Test
    void timeoutReturnsNullAndLeavesLine() throws Exception{
        Keyspace ks = new Keyspace();
        long start = System.nanoTime();
        Popped p = ks.blpop("q", 200, CONNECTED);
        long ms = elapsedMs(start);
        assertNull(p, "returns null");
        assertTrue(ms >= 150 && ms < 1500, "waited about the timeout (" + ms + " ms)");
        assertEquals(0, ks.waitersFor("q"), "line is empty afterwards");
    }

    @Test
    void lpushWakesBlockedClient() throws Exception{
        pushWakesBlockedClient(true);
    }

    @Test
    void rpushWakesBlockedClient() throws Exception{
        pushWakesBlockedClient(false);
    }

    // A push with a client waiting goes straight to that client: the push reports
    // length 1, the client wakes with the value, and the list never holds it.
    private static void pushWakesBlockedClient(boolean left) throws Exception{
        Keyspace ks = new Keyspace();
        Blocked client = new Blocked(ks, "q", 5000, CONNECTED);
        assertTrue(awaitWaiters(ks, "q", 1), "client joined the line");

        long start = System.nanoTime();
        long len = left ? ks.lpush("q", b("hello")) : ks.rpush("q", b("hello"));
        assertEquals(1L, len, "push replies length 1");
        assertTrue(client.finish(2000), "client woke");
        long ms = elapsedMs(start);
        assertTrue(ms < 2000, "woke on the push, not the 5 s timeout (" + ms + " ms)");
        assertPopped("q", "hello", client.result.get());
        assertEquals(0L, ks.llen("q"), "value was never stored in the list");
        assertEquals(0, ks.waitersFor("q"), "line is empty");
    }

    // The client that blocked first is served first.
    @Test
    void fifoAcrossWaiters() throws Exception{
        Keyspace ks = new Keyspace();
        Blocked first = new Blocked(ks, "q", 5000, CONNECTED);
        assertTrue(awaitWaiters(ks, "q", 1), "first client joined");
        Blocked second = new Blocked(ks, "q", 5000, CONNECTED);
        assertTrue(awaitWaiters(ks, "q", 2), "second client joined");

        ks.rpush("q", b("one"));
        ks.rpush("q", b("two"));
        first.finish(2000);
        second.finish(2000);
        assertPopped("q", "one", first.result.get());
        assertPopped("q", "two", second.result.get());
    }

    // A client that gave up must not take a later push away from one still waiting.
    @Test
    void timedOutWaiterDoesNotSwallowPush() throws Exception{
        Keyspace ks = new Keyspace();
        Blocked quitter = new Blocked(ks, "q", 200, CONNECTED);
        assertTrue(awaitWaiters(ks, "q", 1), "short-timeout client joined");
        Blocked stayer = new Blocked(ks, "q", 5000, CONNECTED);
        assertTrue(awaitWaiters(ks, "q", 2), "long-timeout client joined");

        assertTrue(quitter.finish(2000), "short-timeout client finished");
        assertNull(quitter.result.get(), "and got nothing");

        ks.lpush("q", b("x"));
        stayer.finish(2000);
        assertPopped("q", "x", stayer.result.get());
    }

    // ------------------------------------------------------------------
    // types
    // ------------------------------------------------------------------

    // BLPOP on a string is an error straight away -- it must not block.
    @Test
    void wrongTypeFailsImmediately() throws Exception{
        Keyspace ks = new Keyspace();
        ks.set("s", b("hi"));
        assertThrows(WrongTypeException.class, () -> ks.blpop("s", 5000, CONNECTED));
        assertEquals(0, ks.waitersFor("s"), "nobody joined the line");
    }

    // A client is blocked on q, then q is SET to a string. A later LPUSH must be
    // refused with WRONGTYPE -- not hand its value to the waiting client.
    @Test
    void pushToKeyThatBecameStringIsRefused() throws Exception{
        Keyspace ks = new Keyspace();
        Blocked client = new Blocked(ks, "q", 500, CONNECTED);
        assertTrue(awaitWaiters(ks, "q", 1), "client joined the line");

        ks.set("q", b("hi"));
        assertThrows(WrongTypeException.class, () -> ks.lpush("q", b("x")));
        assertTrue(client.finish(2000), "client timed out");
        assertNull(client.result.get(), "waiting client was not handed the value");
    }

    // ------------------------------------------------------------------
    // clients that leave
    // ------------------------------------------------------------------

    // The client disconnects while blocked with timeout 0, so no timeout would
    // ever end the wait. Between slices the waiter notices, leaves the line, and a
    // later push goes to the list instead of to a client that is gone.
    @Test
    void disconnectedWaiterLeavesLine() throws Exception{
        Keyspace ks = new Keyspace();
        AtomicBoolean gone = new AtomicBoolean(false);
        Blocked client = new Blocked(ks, "q", 0, gone::get);
        assertTrue(awaitWaiters(ks, "q", 1), "client joined the line");

        gone.set(true);
        assertTrue(client.finish(3000), "noticed within a slice or two");
        assertInstanceOf(ClientGoneException.class, client.error.get(), "reported the client as gone");
        assertEquals(0, ks.waitersFor("q"), "left the line");

        ks.lpush("q", b("v"));
        assertEquals(1L, ks.llen("q"), "a later push stays in the list");
    }

    // Interrupting a blocked client (what a shutdown would do) must take it out of
    // the line, or a later push would be handed to a thread that's no longer there.
    @Test
    void interruptedWaiterLeavesLine() throws Exception{
        Keyspace ks = new Keyspace();
        Blocked client = new Blocked(ks, "q", 0, CONNECTED);
        assertTrue(awaitWaiters(ks, "q", 1), "client joined the line");

        client.thread.interrupt();
        assertTrue(client.finish(2000), "stopped waiting");
        assertInstanceOf(InterruptedException.class, client.error.get(), "blpop threw InterruptedException");
        assertEquals(0, ks.waitersFor("q"), "left the line");

        ks.lpush("q", b("v"));
        assertEquals(1L, ks.llen("q"), "a later push stays in the list");
    }

    // The data-loss case: a push is handed to a client that disconnected moments
    // earlier. BLPOP must notice before the reply is written, put the value
    // back on the list, and the connection must close.
    @Test
    void valueForDisconnectedClientIsPutBack() throws Exception{
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
        assertTrue(awaitWaiters(ks, "q", 1), "client joined the line");

        // Disconnect, then push within the same 1 s slice, so the push reaches the
        // waiter before blpop's own between-slices check would notice.
        gone.set(true);
        ks.lpush("q", b("v"));

        connection.join(2000);
        assertFalse(connection.isAlive(), "dispatch returned");
        assertInstanceOf(IOException.class, error.get(), "connection closed with IOException");
        assertEquals(0, out.size(), "no reply written to the dead client");
        assertEquals(1L, ks.llen("q"), "value put back on the list");
    }

    // The bug that `printf 'RPUSH jobs a\nBLPOP jobs 0\n' | nc` exposed. A client
    // that has finished sending (but is still reading) looks "gone" to the socket
    // check. With data already in the list BLPOP never waited, so there was no
    // time for the client to leave -- it must get its reply, not a closed
    // connection.
    @Test
    void immediateReplyEvenWhenClientLooksGone() throws Exception{
        Keyspace ks = new Keyspace();
        Dispatcher d = new Dispatcher(ks);
        ks.rpush("q", b("a"));
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        d.dispatch(new Command(new byte[][]{ b("BLPOP"), b("q"), b("0") }), out, () -> true);

        assertEquals("*2\r\n$1\r\nq\r\n$1\r\na\r\n", out.toString(StandardCharsets.ISO_8859_1), "reply written");
        assertEquals(0L, ks.llen("q"), "value delivered, not put back");
    }

    // The same rule inside Keyspace: when BLPOP returns straight away, the
    // "is the client gone?" check must not run at all.
    @Test
    void immediatePopNeverChecksForDisconnect() throws Exception{
        Keyspace ks = new Keyspace();
        ks.rpush("q", b("a"));
        BooleanSupplier mustNotBeCalled = () -> {
            throw new AssertionError("clientGone was checked although BLPOP never waited");
        };
        assertPopped("q", "a", ks.blpop("q", 5000, mustNotBeCalled));
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

    private static long elapsedMs(long startNanos){
        return (System.nanoTime() - startNanos) / 1_000_000;
    }
}
