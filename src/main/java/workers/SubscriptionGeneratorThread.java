package workers;

import models.Subscription;

import java.util.ArrayList;
import java.util.List;

public class SubscriptionGeneratorThread extends Thread {
    private int rate;
    private String metaData;
    private int noOfSubs;
    private List<Subscription> subscriptions;

    public SubscriptionGeneratorThread(String metaData, int rate, int noOfSubs) {
        this.metaData = metaData;
        this.rate = rate;
        this.noOfSubs = noOfSubs;
        this.subscriptions = new ArrayList<>();
    }

    @Override
    public void run() {

        int actualItems = (rate*noOfSubs)/100;

        for (int i = 0; i < actualItems; i++) {
            Subscription subscription = new Subscription();
            subscription.addInfo(metaData, "Da");
            subscriptions.add(subscription);
        }
    }

    public List<Subscription> getSubscriptions() {
        return subscriptions;
    }
}