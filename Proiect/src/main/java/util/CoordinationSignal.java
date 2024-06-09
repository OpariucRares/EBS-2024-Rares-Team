package util;

import java.io.Serializable;
import java.util.concurrent.CountDownLatch;

public class CoordinationSignal implements Serializable {
    private static CountDownLatch latch;

    public static void initializeLatch(int numberOfSubscribers) {
        latch = new CountDownLatch(numberOfSubscribers);
    }

    public static void setAllSubscriptionsSent() {
        latch.countDown();
    }

    public static void waitForAllSubscriptions() throws InterruptedException {
        latch.await();
    }
}

