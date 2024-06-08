package storm;

import models.publication.Publication;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

public class SubscriberBolt extends BaseRichBolt {
    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
    }

    @Override
    public void execute(Tuple tuple) {
        Publication publication = (Publication) tuple.getValueByField("publication");

        System.out.println("Subscriber received publication: " + publication.toString());
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
}