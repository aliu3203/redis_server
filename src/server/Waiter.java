package server;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

// One client blocked in BLPOP.
//
// The state field is the whole point: a waiter can be timing out at the exact
// moment a pusher is handing it a value. Both sides CAS from PENDING, and
// exactly one wins. If the PUSHER loses, it must not drop the element -- it has
// to try the next waiter, or fall back to storing it in the list.
public final class Waiter{

    enum State{ PENDING, FULFILLED, CANCELLED }

    private final AtomicReference<State> state = new AtomicReference<>(State.PENDING);

    // Capacity 1: exactly one delivery ever happens.
    private final ArrayBlockingQueue<Object[]> slot = new ArrayBlockingQueue<>(1);

    // Called by a pushing thread. Returns false if this waiter was already
    // fulfilled or cancelled, in which case the caller still owns the element.
    boolean tryDeliver(String key, byte[] value){
        // TODO: CAS PENDING -> FULFILLED, then slot.offer(new Object[]{key, value})
        return false;
    }

    // Called by the blocked thread on timeout or on disconnect cleanup.
    // Returns false if a pusher already claimed it -- meaning a value is on its
    // way and must still be consumed rather than lost.
    boolean cancel(){
        // TODO: CAS PENDING -> CANCELLED
        return false;
    }

    // Blocks up to timeoutMs. Returns {key, value}, or null on timeout.
    // timeoutMs <= 0 means block indefinitely (BLPOP's "0").
    Object[] await(long timeoutMs) throws InterruptedException{
        // TODO: slot.poll(timeoutMs, TimeUnit.MILLISECONDS), or slot.take() for 0
        return null;
    }
}
