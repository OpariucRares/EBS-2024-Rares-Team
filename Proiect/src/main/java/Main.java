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
import java.util.Arrays;
import java.util.Collections;
import util.CoordinationSignal;

public class Main {
    public static void main(String[] args) throws Exception {
        Constants constants = new Constants();

        SubscriptionGenerator subscriptionGenerator = new SubscriptionGenerator();
        var subscriptions = subscriptionGenerator.generateSubscriptions(10, constants.fieldFreq, constants.eqFreq);

        PublicationGenerator publicationGenerator = new PublicationGenerator();
        var publications = publicationGenerator.generatePublications(10, constants.pubFieldFreq);
//        var publications2 = publicationGenerator.generatePublications(100000, constants.pubFieldFreq);

        // One PublisherSpout instance might mean duplicated values
        PublisherSpout publisherSpout1 = new PublisherSpout(publications);
        PublisherSpout publisherSpout2 = new PublisherSpout(publications);
        BrokerBolt brokerBolt1 = new BrokerBolt();
        BrokerBolt brokerBolt2 = new BrokerBolt();
        BrokerBolt brokerBolt3 = new BrokerBolt();
        SubscriberBolt subscriberBolt1 = new SubscriberBolt("subscriber-123", subscriptions);
        SubscriberBolt subscriberBolt2 = new SubscriberBolt("subscriber-456", subscriptions);
        SubscriberBolt subscriberBolt3 = new SubscriberBolt("subscriber-789", subscriptions);

        TopologyBuilder builder = new TopologyBuilder();

        // Adăugarea PublisherSpout la topologie
        builder.setSpout("publisher-spout-1", publisherSpout1, 2);
        // builder.setSpout("publisher-spout2", publisherSpout2, 2);

        // Adăugarea BrokerBolt la topologie
        builder.setBolt("broker-bolt-1", brokerBolt1, 3)
                .shuffleGrouping("publisher-spout-1");

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
            Thread.sleep(6000);

            // Shutdown the local cluster
            cluster.shutdown();
        } else {
            // Submit the topology to the Storm cluster
            StormSubmitter.submitTopology(args[0], config, builder.createTopology());
        }
    }
}