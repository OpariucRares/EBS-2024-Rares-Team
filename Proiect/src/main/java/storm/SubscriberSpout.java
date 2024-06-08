package storm;

import models.publication.PublicationField;
import models.subscription.Subscription;
import models.subscription.SubscriptionField;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Date;
import java.util.List;
import java.util.Map;

public class SubscriberSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private List<Subscription> subscriptions;
    private int index;

    public SubscriberSpout(List<Subscription> subscriptions) {
        this.subscriptions = subscriptions;
        this.index = 0;
    }

    @Override
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        if (index < subscriptions.size()) {
            String fieldName = null;
            String operator = null;
            Object value = null;

            Subscription subscription = subscriptions.get(index++);
            for (SubscriptionField field : subscription.getFields()) {
                fieldName = field.getFieldName();
                operator = field.getOperator();
                value = field.getValue();
            }

            collector.emit(new Values(fieldName, operator, value));
            StringBuilder sb = new StringBuilder();
            sb.append("Subscription -> fieldName: ").append(fieldName).append(" operator ").
                    append(operator).append(" Value ").append(value);
            System.out.println(sb);
            try {
                Thread.sleep(1000); // Sleep briefly between emits
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        } else {
            try {
                Thread.sleep(1000); // Sleep when there are no more subscriptions to emit
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("fieldName", "operator", "value"));
    }
}