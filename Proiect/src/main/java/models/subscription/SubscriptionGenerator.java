package models.subscription;

import org.apache.commons.math3.distribution.EnumeratedDistribution;
import org.apache.commons.math3.util.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

public class SubscriptionGenerator {
    private static final String[] OPERATORS = {"=", ">", "<"};

    public List<Subscription> generateSubscriptions(
            int count, Map<String, Double> fieldFreq, Map<String, Double> eqFreq) {

        List<Subscription> subscriptions = new ArrayList<>();

        int subscriptionLine = 0;

        for (int i = 0; i < count; i++) {
            subscriptions.add(new Subscription());
        }

        for (var fieldFreqEntry : fieldFreq.entrySet()) {
            String fieldName = fieldFreqEntry.getKey();
            Double freq = fieldFreqEntry.getValue();
            int fieldCount = (int) (count * freq);

            new SubscriptionsWorker(
                    count, subscriptions, fieldName, subscriptionLine, fieldCount,
                    buildDistribution(eqFreq.getOrDefault(fieldName, null))
            ).run();

            subscriptionLine += fieldCount;
        }

        return subscriptions;
    }

    private static SynchronizedEnumeratedDistribution buildDistribution(Double eqFreq) {
        if (eqFreq == null) {
            eqFreq = 1.0 / OPERATORS.length;
        }

        List<Pair<String, Double>> operatorProbabilities = new ArrayList<>();
        double otherOperatorsProb = (1.0 - eqFreq) / (OPERATORS.length - 1);

        operatorProbabilities.add(new Pair<>("=", eqFreq));

        for (String operator : OPERATORS) {
            if (!operator.equals("=")) {
                operatorProbabilities.add(new Pair<>(operator, otherOperatorsProb));
            }
        }

        return new SynchronizedEnumeratedDistribution(new EnumeratedDistribution<>(operatorProbabilities));
    }
}
