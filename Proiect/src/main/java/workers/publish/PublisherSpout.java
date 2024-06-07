package workers.publish;

import models.Publication;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class PublisherSpout implements IRichSpout {
    private SpoutOutputCollector collector;
    private boolean isDistributed;

    public PublisherSpout(boolean isDistributed) {
        this.isDistributed = isDistributed;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        String publication = new Publication().toString();
        collector.emit(new Values(publication));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("publication"));
    }

    @Override
    public void close() {}

    @Override
    public void activate() {}

    @Override
    public void deactivate() {}

    @Override
    public void ack(Object msgId) {}

    @Override
    public void fail(Object msgId) {}

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
