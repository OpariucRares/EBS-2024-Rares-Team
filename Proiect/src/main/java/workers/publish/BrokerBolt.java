package workers.publish;

import models.Subscription;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.*;

public class BrokerBolt implements IRichBolt {
    private OutputCollector collector;
    private Map<String, List<Subscription>> subscriptionsMap;
    private List<String> subscribers;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.subscriptionsMap = new HashMap<>();
        this.subscribers = new ArrayList<>();
    }

    @Override
    public void execute(Tuple input) {
        String sourceComponent = input.getSourceComponent();
        if (sourceComponent.equals("subscriber")) {
            String subscriberId = input.getStringByField("subscriberId");
            Subscription subscription = (Subscription) input.getValueByField("subscription");
            subscriptionsMap.computeIfAbsent(subscriberId, k -> new ArrayList<>()).add(subscription);
            if (!subscribers.contains(subscriberId)) {
                subscribers.add(subscriberId);
            }
        } else if (sourceComponent.equals("publisher")) {
            String publication = input.getStringByField("publication");
            for (String subscriber : subscribers) {
                for (Subscription subscription : subscriptionsMap.get(subscriber)) {
                    if (subscription.matches(publication)) {
                        collector.emit(new Values(subscriber, publication));
                    }
                }
            }
        }
        collector.ack(input);
    }

    @Override
    public void cleanup() {}

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("subscriberId", "publication"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}

