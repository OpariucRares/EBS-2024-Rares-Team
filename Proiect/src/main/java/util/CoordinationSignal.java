package util;

public class CoordinationSignal {
    private boolean allSubscriptionsSent = false;

    public synchronized void setAllSubscriptionsSent() {
        allSubscriptionsSent = true;
        notifyAll();
    }

    public synchronized void waitForAllSubscriptions() throws InterruptedException {
        while (!allSubscriptionsSent) {
            wait();
        }
    }
}

