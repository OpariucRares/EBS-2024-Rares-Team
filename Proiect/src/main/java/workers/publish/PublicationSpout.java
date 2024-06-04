package workers.publish;

import models.Publication;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.Random;

public class PublicationSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private Random random;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        this.random = new Random();
    }

    @Override
    public void nextTuple() {
        Publication publication = new Publication();
        collector.emit(new Values(publication.toString()));
        try {
            Thread.sleep(1000);  // Emit a publication every second
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        //declarer.declare(new Fields("publication"));
    }
}