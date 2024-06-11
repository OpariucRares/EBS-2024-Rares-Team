import models.publication.Publication;
import models.publication.PublicationField;
import models.subscription.Subscription;
import models.subscription.SubscriptionField;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import models.publication.PublicationGenerator;
import models.subscription.SubscriptionGenerator;
import org.apache.storm.tuple.Fields;
import storm.BrokerBolt;
import storm.PublisherSpout;
import storm.SubscriberBolt;
import util.Constants;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

public class Main {
    public static void main(String[] args) throws Exception {
        Constants constants = new Constants();

        SubscriptionGenerator subscriptionGenerator = new SubscriptionGenerator();

        var subscriptions1 = subscriptionGenerator.generateSubscriptions(5000, constants.fieldFreq, constants.eqFreq);
        try (BufferedWriter writer = new BufferedWriter(new FileWriter("results/subscriptions1.txt"))) {
            for (var subscription : subscriptions1) {
                writer.write(subscription.toString());
                writer.newLine();
            }
            System.out.println("Subscriptions written to subscriptions.txt");
        } catch (IOException e) {
            System.err.println("Error writing to file: " + e.getMessage());
        }

        var subscriptions2 = subscriptionGenerator.generateSubscriptions(5000, constants.fieldFreq, constants.eqFreq);
        try (BufferedWriter writer = new BufferedWriter(new FileWriter("results/subscriptions2.txt"))) {
            for (var subscription : subscriptions2) {
                writer.write(subscription.toString());
                writer.newLine();
            }
            System.out.println("Subscriptions written to subscriptions.txt");
        } catch (IOException e) {
            System.err.println("Error writing to file: " + e.getMessage());
        }

        PublicationGenerator publicationGenerator = new PublicationGenerator();
        var publications = publicationGenerator.generatePublications(300, constants.pubFieldFreq);

        try (BufferedWriter writer = new BufferedWriter(new FileWriter("results/publications.txt"))) {
            for (var publication : publications) {
                writer.write(publication.toString());
                writer.newLine();
            }
            System.out.println("Publications written to publications.txt");
        } catch (IOException e) {
            System.err.println("Error writing to file: " + e.getMessage());
        }

        PublisherSpout publisherSpout1 = new PublisherSpout(publications);
//        PublisherSpout publisherSpout2 = new PublisherSpout(publications);
        BrokerBolt brokerBoltDecode = new BrokerBolt("broker-decode");
        BrokerBolt brokerBolt1 = new BrokerBolt("broker1");
        BrokerBolt brokerBolt2 = new BrokerBolt("broker2");
        BrokerBolt brokerBolt3 = new BrokerBolt("broker3");
        SubscriberBolt subscriberBolt1 = new SubscriberBolt("subscriber1", subscriptions1);
        SubscriberBolt subscriberBolt2 = new SubscriberBolt("subscriber2", subscriptions2);
//        SubscriberBolt subscriberBolt3 = new SubscriberBolt("subscriber-789", subscriptions);

        TopologyBuilder builder = new TopologyBuilder();

        // Adăugarea PublisherSpout la topologie
        builder.setSpout("publisher-spout-1", publisherSpout1, 2);
        // builder.setSpout("publisher-spout2", publisherSpout2, 2);

        // Adăugarea BrokerBolt la topologie
        builder.setBolt("broker-bolt-decode", brokerBoltDecode, 3)
                        .shuffleGrouping("publisher-spout-1");
                        // .fieldsGrouping("broker-bolt-1", "decoded-stream", new Fields("company", "value", "drop", "variation", "date"));

        builder.setBolt("broker-bolt-1", brokerBolt1, 3)
                .shuffleGrouping("broker-bolt-decode", "decoded-stream")
                .fieldsGrouping("broker-bolt-2", "subscription-stream", new Fields("subscriberId"))
                .fieldsGrouping("broker-bolt-3", "subscription-stream", new Fields("subscriberId"));

        builder.setBolt("broker-bolt-2", brokerBolt2, 3)
                .shuffleGrouping("broker-bolt-1", "notification-stream")
                .fieldsGrouping("subscriber-bolt-1", "subscription-stream", new Fields("subscriberId"));

        builder.setBolt("broker-bolt-3", brokerBolt3, 3)
                .shuffleGrouping("broker-bolt-1", "notification-stream")
                .fieldsGrouping("subscriber-bolt-2", "subscription-stream", new Fields("subscriberId"));

        // Adăugarea SubscriberBolt la topologie
        builder.setBolt("subscriber-bolt-1", subscriberBolt1, 2)
                .shuffleGrouping("broker-bolt-2", "notification-stream");

        builder.setBolt("subscriber-bolt-2", subscriberBolt2, 2)
                .shuffleGrouping("broker-bolt-3", "notification-stream");

        // Config
        Config config = new Config();
        config.setDebug(true);

        config.setNumWorkers(3);

        config.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList("localhost"));
        config.put(Config.STORM_ZOOKEEPER_PORT, 2181);

        config.registerSerialization(java.util.Date.class);
        config.registerSerialization(Subscription.class);
        config.registerSerialization(Publication.class);
        config.registerSerialization(PublicationField.class);
        config.registerSerialization(SubscriptionField.class);
        config.registerSerialization(Collections.unmodifiableList(Collections.emptyList()).getClass());
        config.registerSerialization(Collections.synchronizedList(Collections.emptyList()).getClass());
        config.registerSerialization(Collections.unmodifiableMap(Collections.emptyMap()).getClass());
        config.registerSerialization(Collections.synchronizedMap(Collections.emptyMap()).getClass());


        //config.registerSerialization(models.subscription.Subscription.class);
        //config.registerSerialization(java.util.Collections.class);

        if (args.length == 0) {
            // Run the topology in a local cluster
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("word-count-topology-one", config, builder.createTopology());

            // Keep the topology running for some time (e.g., 60 seconds) for demonstration purposes
//            Thread.sleep(60000 * 3); // multiplied by the number of minutes wanted
            Thread.sleep(6000 * 3);

            // Shutdown the local cluster
            cluster.shutdown();
        } else {
            // Submit the topology to the Storm cluster
            StormSubmitter.submitTopology(args[0], config, builder.createTopology());
        }
    }
}