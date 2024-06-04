import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import workers.publish.PublicationBolt;
import workers.publish.PublicationSpout;

public class Main {
    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("publication-spout", new PublicationSpout());
        builder.setBolt("publication-bolt", new PublicationBolt()).shuffleGrouping("publication-spout");

        Config config = new Config();
        config.setDebug(true);

        LocalCluster cluster = new LocalCluster();
        try {
            cluster.submitTopology("publication-topology", config, builder.createTopology());
            Thread.sleep(10000);  // Run the topology for 10 seconds
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            cluster.shutdown();
        }
    }
}