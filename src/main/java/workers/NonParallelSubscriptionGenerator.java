package workers;

import models.Subscription;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static algorithm.PubSubAlgorithm.minimumCompany;

public class NonParallelSubscriptionGenerator {

    private int rate;
    private String metaData;
    private int noOfSubs;
    private List<Subscription> subscriptions;

    private final List<String> companyOperators = List.of("=", "!=");

    private final List<String> otherOperators = List.of("<", "<=", "=", ">", ">=");

    private final double MIN_VALUE = 0.0d;
    private final double MAX_VALUE = 100.0d;

    public NonParallelSubscriptionGenerator(String metaData, int rate, int noOfSubs) {
        this.metaData = metaData;
        this.rate = rate;
        this.noOfSubs = noOfSubs;
        this.subscriptions = new ArrayList<>();
    }

    public void generate() {

        int actualItems = (rate*noOfSubs)/100;
        Random random = new Random();

        for (int i = 0; i < actualItems; i++) {
            Subscription subscription = new Subscription();
            if(metaData == "Company")
            {
                final List<String> companies = Arrays.asList("Facebook", "Amazon", "Netflix", "Google");

                int indexCompany = random.nextInt(companies.size());
                int localOperator;
                if (random.nextInt(100) > minimumCompany) {
                    localOperator = random.nextInt(companyOperators.size());
                }
                else
                {
                    localOperator = 0;
                }

                subscription.addOperator(companyOperators.get(localOperator));
                subscription.addInfo(metaData, companies.get(indexCompany));
            }
            else if(metaData == "Date")
            {
                LocalDate start = LocalDate.of(2023, 1, 1);
                LocalDate end = LocalDate.of(2024, 3, 31);
                long startDateEpochDay = start.toEpochDay();
                long endDateEpochDay = end.toEpochDay();
                long randomDateEpochDay = startDateEpochDay + random.nextInt((int) (endDateEpochDay - startDateEpochDay));
                LocalDate randomDate = LocalDate.ofEpochDay(randomDateEpochDay);

                int localOperator = random.nextInt(otherOperators.size());

                subscription.addOperator(otherOperators.get(localOperator));
                subscription.addInfo(metaData, java.sql.Date.valueOf(randomDate).toString());
            }
            else
            {
                final double value = Math.round((MIN_VALUE + (MAX_VALUE - MIN_VALUE) * random.nextDouble()) * 100.0) / 100.0;

                int localOperator = random.nextInt(otherOperators.size());

                subscription.addOperator(otherOperators.get(localOperator));
                subscription.addInfo(metaData, String.valueOf(value));
            }
            subscriptions.add(subscription);
        }
    }

    public List<Subscription> getSubscriptions() {
        return subscriptions;
    }

}
