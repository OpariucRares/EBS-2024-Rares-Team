package models.subscription;

import org.apache.commons.math3.distribution.EnumeratedDistribution;

public class SynchronizedEnumeratedDistribution {
    private final EnumeratedDistribution<String> distribution;

    public SynchronizedEnumeratedDistribution(EnumeratedDistribution<String> distribution) {
        this.distribution = distribution;
    }

    public synchronized String sample() {
        return distribution.sample();
    }
}
