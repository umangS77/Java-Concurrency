package Concurrency.OddEven;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/*

Enhancement : Configurable N Threads (Turn-Based)

N threads
round-robin turn

=> Turn = (num−1) % threadCount.

 */
public class OrderedPrint2 {
    private int MAX;
    private int maxThreads;
    private int num = 1;

    private ReentrantLock lock = new ReentrantLock();
    private Condition condition = lock.newCondition();

    public OrderedPrint2(int MAX, int maxThreads) {
        this.MAX = MAX;
        this.maxThreads = maxThreads;
    }

    public void worker(int id) {
        while (true) {
            lock.lock();
            try {
                while (num <= MAX && (num-1) % maxThreads != id) {
                    condition.awaitUninterruptibly();
                }

                if (num > MAX) return;

                System.out.println("ThreadId : " + id + " --- num : " + num);
                num++;
                condition.signalAll();
            } finally {
                lock.unlock();
            }
        }
    }
}
