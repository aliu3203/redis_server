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
    private final ArrayBlockingQueue<Popped> slot = new ArrayBlockingQueue<>(1);

    // Called by a pushing thread. Returns false if this waiter was already
    // fulfilled or cancelled, in which case the caller still owns the element.
    boolean tryDeliver(String key, byte[] value){
        if(!state.compareAndSet(State.PENDING, State.FULFILLED)){
            return false;                         // already claimed: pusher keeps the value
        }
        slot.offer(new Popped(key, value));       // we own it now, so deliver
        return true;
    }

    // Called by the blocked thread on timeout or on disconnect cleanup.
    // Returns false if a pusher already claimed it -- meaning a value is on its
    // way and must still be consumed rather than lost.
    boolean cancel(){
        return state.compareAndSet(State.PENDING, State.CANCELLED);
    }

    // Blocks up to timeoutMs. Returns {key, value}, or null on timeout.
    // timeoutMs <= 0 means block indefinitely (BLPOP's "0").
    Popped await(long timeoutMs) throws InterruptedException{
        if(timeoutMs > 0){
            return slot.poll(timeoutMs, TimeUnit.MILLISECONDS);   // null on timeout
        }
        return slot.take();
    }
}
