package models.subscription;

import java.util.Date;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class SubscriptionsWorker implements Runnable {
    private static final String[] COMPANIES = {"Google", "Microsoft", "Apple", "Amazon"};
    private static final double PRECISION = 100.0;

    private final Integer subscriptionCount;
    private final List<Subscription> subscriptionList;
    private Integer subscriptionLine;
    private final String fieldName;
    private final Integer fieldCount;

    private final SynchronizedEnumeratedDistribution distribution;

    public SubscriptionsWorker(Integer subscriptionCount, List<Subscription> subscriptionList,
                               String fieldName, Integer subscriptionLine, Integer fieldCount,
                               SynchronizedEnumeratedDistribution distribution) {
        this.subscriptionCount = subscriptionCount;
        this.subscriptionList = subscriptionList;
        this.fieldName = fieldName;
        this.subscriptionLine = subscriptionLine;
        this.fieldCount = fieldCount;
        this.distribution = distribution;
    }

    @Override
    public void run() {
        for (int i = 0; i < fieldCount; i++) {
            Object fieldValue;

            if (fieldName.equals("company")){
                fieldValue = COMPANIES[ThreadLocalRandom.current().nextInt(0, COMPANIES.length)];
            } else if(fieldName.equals("date")){
                fieldValue = getRandomDate();
            }
            else{
                fieldValue = Math.round(ThreadLocalRandom.current().nextDouble() * PRECISION) / PRECISION;
            }

            subscriptionList.get(subscriptionLine % subscriptionCount).addField(
                    new SubscriptionField(
                            fieldName,
                            getRandomOperator(),
                            fieldValue
                    )
            );
            this.subscriptionLine++;
        }
    }

    private static Date getRandomDate() {
        int year = ThreadLocalRandom.current().nextInt(2000, 2023) - 1900;
        int month = ThreadLocalRandom.current().nextInt(0, 12);
        int day = ThreadLocalRandom.current().nextInt(1, 32);
        return new Date(year, month, day);
    }

    private String getRandomOperator() {
        return this.distribution.sample();
    }
}
