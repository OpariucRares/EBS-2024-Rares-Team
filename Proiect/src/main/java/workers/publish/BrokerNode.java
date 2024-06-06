package workers.publish;

import models.Subscription;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import util.Constants;

public class BrokerNode {
    private BrokerBolt brokerBolt;
    private int noOfSubs;
    private boolean isParallel;
    private int companyRate;
    private int valueRate;
    private int dropRate;
    private int variationRate;
    private int dateRate;
    private List<Subscription> generatedSubscriptions;

    public BrokerNode(BrokerBolt brokerBolt, int noOfSubs, boolean isParallel, int companyRate, int valueRate, int dropRate, int variationRate, int dateRate) {
        this.brokerBolt = brokerBolt;
        this.noOfSubs = noOfSubs;
        this.isParallel = isParallel;
        this.companyRate = companyRate;
        this.valueRate = valueRate;
        this.dropRate = dropRate;
        this.variationRate = variationRate;
        this.dateRate = dateRate;
    }

    public void generateSubscriptions() {
        List<String> metadatas = util.Constants.metadataKeys;
        List<Integer> threadRates = Arrays.asList(companyRate, valueRate, dropRate, variationRate, dateRate);

        List<SubscriptionGeneratorThread> availableThreads = new ArrayList<>();
        for (int i = 0; i < metadatas.size(); i++) {
            SubscriptionGeneratorThread thread = new SubscriptionGeneratorThread(metadatas.get(i), threadRates.get(i), noOfSubs);
            availableThreads.add(thread);
            thread.start();
        }

        for (SubscriptionGeneratorThread thread : availableThreads) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        int subsCounter = 0;

        for (SubscriptionGeneratorThread thread : availableThreads) {
            if (thread.getSubscriptions().size() + subsCounter <= noOfSubs) {
                generatedSubscriptions.addAll(thread.getSubscriptions());
                subsCounter += thread.getSubscriptions().size();
                continue;
            }
            for (Subscription sub : thread.getSubscriptions()) {
                if (subsCounter < noOfSubs) {
                    generatedSubscriptions.add(sub);
                    subsCounter++;
                    continue;
                }

                Random random = new Random();
                boolean ok = false;
                while (!ok) {
                    int randomIndex = random.nextInt(generatedSubscriptions.size());
                    Subscription randomSubscription = generatedSubscriptions.get(randomIndex);

                    for (String key : sub.getInfo().keySet()) {
                        if (!randomSubscription.getInfo().containsKey(key)) {
                            randomSubscription.addInfo(key, sub.getInfo().get(key));
                            randomSubscription.addOperator(sub.getOperator().get(0));
                            ok = true;
                        }

                    }
                }
            }
        }

        long companySubscriptionCount = generatedSubscriptions.stream()
                .filter(subscription -> subscription.getInfo().containsKey(metadatas.get(Constants.getInstance().COMPANY_INDEX)))
                .count();

        long valueSubscriptionCount = generatedSubscriptions.stream()
                .filter(subscription -> subscription.getInfo().containsKey(metadatas.get(Constants.getInstance().VALUE_INDEX)))
                .count();

        long dropSubscriptionCount = generatedSubscriptions.stream()
                .filter(subscription -> subscription.getInfo().containsKey(metadatas.get(Constants.getInstance().DROP_INDEX)))
                .count();

        long variationSubscriptionCount = generatedSubscriptions.stream()
                .filter(subscription -> subscription.getInfo().containsKey(metadatas.get(Constants.getInstance().VARIATION_INDEX)))
                .count();

        long dateSubscriptionCount = generatedSubscriptions.stream()
                .filter(subscription -> subscription.getInfo().containsKey(metadatas.get(Constants.getInstance().DATE_INDEX)))
                .count();

        System.out.println("Number of subscriptions containing 'Company' key: " + companySubscriptionCount);
        System.out.println("Number of subscriptions containing 'Value' key: " + valueSubscriptionCount);
        System.out.println("Number of subscriptions containing 'Drop' key: " + dropSubscriptionCount);
        System.out.println("Number of subscriptions containing 'Variation' key: " + variationSubscriptionCount);
        System.out.println("Number of subscriptions containing 'Date' key: " + dateSubscriptionCount);

        // Register subscriptions with BrokerBolt
        for (Subscription subscription : generatedSubscriptions) {
            brokerBolt.addSubscription("brokerId", subscription); // Assume brokerId is defined or passed as an argument
        }
    }
}

