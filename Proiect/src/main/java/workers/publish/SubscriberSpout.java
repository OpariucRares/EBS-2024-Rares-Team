package workers.publish;

import models.Subscription;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class SubscriberSpout implements IRichSpout {
    private SpoutOutputCollector collector;
    private String subscriberId;
    private transient Subscription subscription; // Make subscription transient
    private boolean sent;
    private String field;
    private String value;
    private String operator;

    public SubscriberSpout(String subscriberId, String field, String value, String operator) {
        this.subscriberId = subscriberId;
        this.field = field;
        this.value = value;
        this.operator = operator;
        this.sent = false;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        this.subscription = new Subscription();
        this.subscription.addInfo(field, value);
        this.subscription.addOperator(operator);
    }

    @Override
    public void nextTuple() {
        if (!sent) {
            collector.emit(new Values(subscriberId, subscription));
            sent = true;
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("subscriberId", "subscription"));
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


