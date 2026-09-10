package server; 

import java.util.concurrent.locks.ReentrantLock;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import java.util.HashMap;

public final class Stripe{
    final ReentrantLock lock = new ReentrantLock();
    final Map<String, Deque<Waiter>> waiters = new HashMap<>();
}
