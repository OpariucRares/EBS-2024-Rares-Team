import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import workers.publish.PublicationBolt;
import workers.publish.PublicationSpout;

import java.util.Arrays;

public class Main {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("publication-spout", new PublicationSpout());
        builder.setBolt("publication-bolt", new PublicationBolt()).shuffleGrouping("publication-spout");

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