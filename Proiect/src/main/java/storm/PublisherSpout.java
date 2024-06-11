package storm;

import models.publication.Publication;
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

            String company = null;
            double value = 0.0;
            double drop = 0.0;
            double variation = 0.0;
            Date date = null;

            for (PublicationField field : publication.getFields()) {
                switch (field.getFieldName()) {
                    case "company":
                        company = (String) field.getValue();
                        break;
                    case "value":
                        value = (double) field.getValue();
                        break;
                    case "drop":
                        drop = (double) field.getValue();
                        break;
                    case "variation":
                        variation = (double) field.getValue();
                        break;
                    case "date":
                        date = (Date) field.getValue();
                        break;
                }
            }

            long emissionTime = System.currentTimeMillis();
            collector.emit(new Values(company, value, drop, variation, date, emissionTime));
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
        declarer.declare(new Fields("company", "value", "drop", "variation", "date", "emissionTime"));
    }
}
