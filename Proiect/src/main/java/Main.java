import models.Subscription;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import workers.publish.BrokerBolt;
import workers.publish.PublisherSpout;
import workers.publish.SubscriberSpout;

import java.util.Arrays;

public class Main {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        // Publishers
        builder.setSpout("publisher1", new PublisherSpout(true), 1);
        builder.setSpout("publisher2", new PublisherSpout(true), 1);

        // Brokers
        builder.setBolt("broker1", new BrokerBolt(), 1).shuffleGrouping("publisher1").shuffleGrouping("publisher2");
        builder.setBolt("broker2", new BrokerBolt(), 1).shuffleGrouping("publisher1").shuffleGrouping("publisher2");

        // Subscribers
        builder.setSpout("subscriber1", new SubscriberSpout("subscriber1", "field1", "10", ">="), 1);
        builder.setSpout("subscriber2", new SubscriberSpout("subscriber2", "field2", "20", "<"), 1);


        // Config
        Config config = new Config();
        config.setDebug(true);

        config.setNumWorkers(3);

        config.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList("localhost"));
        config.put(Config.STORM_ZOOKEEPER_PORT, 2181);

        if (args.length == 0) {
            // Run the topology in a local cluster
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("word-count-topology", config, builder.createTopology());

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