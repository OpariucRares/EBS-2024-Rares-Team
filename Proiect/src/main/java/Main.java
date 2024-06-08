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
import java.util.Arrays;

public class Main {
    public static void main(String[] args) throws Exception {
        Constants constants = new Constants();

        SubscriptionGenerator subscriptionGenerator = new SubscriptionGenerator();
        var subscriptions = subscriptionGenerator.generateSubscriptions(10, constants.fieldFreq, constants.eqFreq);

        PublicationGenerator publicationGenerator = new PublicationGenerator();
        var publications = publicationGenerator.generatePublications(10, constants.pubFieldFreq);
//        var publications2 = publicationGenerator.generatePublications(100000, constants.pubFieldFreq);


        // One PublisherSpout instance might mean duplicated values
        PublisherSpout publisherSpout = new PublisherSpout(publications);
        PublisherSpout publisherSpout2 = new PublisherSpout(publications);
        BrokerBolt brokerBolt = new BrokerBolt();
        BrokerBolt brokerBolt2 = new BrokerBolt();
        SubscriberBolt subscriberBolt = new SubscriberBolt("subscriber-123", subscriptions);

        TopologyBuilder builder = new TopologyBuilder();

        // Adăugarea PublisherSpout la topologie
        builder.setSpout("publisher-spout", publisherSpout, 2);

        // Adăugarea BrokerBolt la topologie
        builder.setBolt("broker-bolt", brokerBolt, 3)
                .shuffleGrouping("publisher-spout")
                .fieldsGrouping("subscriber-bolt", "subscription-stream", new Fields("subscriberId"));

        // Adăugarea SubscriberBolt la topologie
        builder.setBolt("subscriber-bolt", subscriberBolt, 2)
                .shuffleGrouping("broker-bolt", "notification-stream");

        // Config
        Config config = new Config();
        config.setDebug(true);

        config.setNumWorkers(3);

        config.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList("localhost"));
        config.put(Config.STORM_ZOOKEEPER_PORT, 2181);

        config.registerSerialization(java.util.Date.class);
        //config.registerSerialization(models.subscription.Subscription.class);
        //config.registerSerialization(java.util.Collections.class);

        if (args.length == 0) {
            // Run the topology in a local cluster
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("word-count-topology-one", config, builder.createTopology());

            // Keep the topology running for some time (e.g., 60 seconds) for demonstration purposes
            Thread.sleep(6000);

            // Shutdown the local cluster
            cluster.shutdown();
        } else {
            // Submit the topology to the Storm cluster
            StormSubmitter.submitTopology(args[0], config, builder.createTopology());
        }
    }
}