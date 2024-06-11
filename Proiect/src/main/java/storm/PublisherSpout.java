package storm;

// import models.publication.Publication;
import models.publication.PublicationField;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import java.util.Date;
import java.util.List;
import java.util.Map;
import models.publication.PublicationOuterClass.*;

public class PublisherSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private List<Publication> publications;
    private int index;
    private long startTime;
    private int sentPublicationsNumber;

    public PublisherSpout(List<Publication> publications) {
        this.publications = publications;
        this.index = 0;
        this.sentPublicationsNumber = 0;
    }

    @Override
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        this.startTime = System.currentTimeMillis();
    }

    @Override
    public void nextTuple() {
        if (index < publications.size()) {
            Publication publication = publications.get(index++);
            byte[] serializedPublication = publication.toByteArray();

            long emissionTime = System.currentTimeMillis();
            collector.emit(new Values(serializedPublication, emissionTime));
            sentPublicationsNumber++;
            System.out.println("Publications emitted: " + sentPublicationsNumber);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        } else {
            try {
                Thread.sleep(1000);  // Sleep briefly when all tuples have been emitted
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("publication", "emissionTime"));
    }
}