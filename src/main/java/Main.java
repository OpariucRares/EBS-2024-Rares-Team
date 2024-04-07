import algorithm.PubSubAlgorithm;

import java.time.Duration;
import java.time.Instant;

public class Main {
    public static void main(String[] args) {
        //creating an object with default values
        Instant startTime = Instant.now();
        PubSubAlgorithm pubSubAlgorithm = new PubSubAlgorithm();

        //reading from the input file and getting the values
        pubSubAlgorithm.init();
        pubSubAlgorithm.printInputValues();

        //algorithm of generating
        pubSubAlgorithm.generatePublications();
        pubSubAlgorithm.generateSubscriptions();

        //the result written in files
        pubSubAlgorithm.createPubAndSubFiles();
        Instant endTime = Instant.now();

        Duration duration = Duration.between(startTime, endTime);
        long resultTimeMillis = duration.toMillis();

        System.out.println("Result Time: " + resultTimeMillis + " milliseconds");
    }

}