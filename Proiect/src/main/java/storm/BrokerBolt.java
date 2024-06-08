package storm;

import models.publication.Publication;
import models.publication.PublicationField;
import models.subscription.Subscription;
import models.subscription.SubscriptionField;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class BrokerBolt extends BaseRichBolt {
    private OutputCollector collector;
    private List<Subscription> subscriptions;

    public BrokerBolt() {
        this.subscriptions = new ArrayList<>();
    }

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        String sourceComponent = tuple.getSourceComponent();
        System.out.println("Input source" + sourceComponent);
        if (sourceComponent.equals("subscriber-bolt")) {
            System.out.println("I am subscriber");
            //logica de preluare a valorilor de la spout-ul subscriber
            //String subscriberId = input.getStringByField("subscriberId");
            //SubscriptionDEPRECATED subscriptionDEPRECATED = (SubscriptionDEPRECATED) input.getValueByField("subscription");
            //doar il adauga

//            String subscriberId = tuple.getStringByField("subscriberId");
//            Subscription subscription = (Subscription) tuple.getValueByField("subscription");
//            // Adăugarea subscripției la lista internă, asociată cu subscriberId
//            subscriptions.add(subscription);
        }
        else if (sourceComponent.equals("publisher-spout")) {
            System.out.println("I am publisher");

//            Publication publication = (Publication) tuple.getValueByField("publication");
//            // Verificarea potrivirii publicației cu fiecare subscripție
//            for (Subscription subscription : subscriptions) {
//                if (subscription.matches(publication)) {
//                    // Emiterea notificării către SubscriberBolt corespunzător
//                    collector.emit(new Values(subscription.getSubscriberId(), publication));
//                }
//            }

            //cauti in lista de subscriberi -> gasesti, faci match
            //String publication = input.getStringByField("publication");
            //            for (String subscriber : subscribers) {
            //                for (SubscriptionDEPRECATED subscriptionDEPRECATED : subscriptionsMap.get(subscriber)) {
            //                    if (subscriptionDEPRECATED.matches(publication)) {
                                    //TODO: AICI ESTI PE NOTIFICATION STREAM CAND SE FACE MATCH-UL CORECT
            //                         collector.emit("notification-stream", new Values(subscription.getSubscriberId(), publication));
            //                    }
            //                }
            //            }

            String company = tuple.getStringByField("company");
            double value = tuple.getDoubleByField("value");
            double drop = tuple.getDoubleByField("drop");
            double variation = tuple.getDoubleByField("variation");
            Date date = (Date) tuple.getValueByField("date");

            Publication publication = new Publication();
            publication.addField(new PublicationField("company", company));
            publication.addField(new PublicationField("value", value));
            publication.addField(new PublicationField("drop", drop));
            publication.addField(new PublicationField("variation", variation));
            publication.addField(new PublicationField("date", date));

            for (Subscription subscription : subscriptions) {
                if (matches(subscription, publication)) {
                    collector.emit(new Values(publication));
                }
            }
        }
    }

    private boolean matches(Subscription subscription, Publication publication) {
        for (SubscriptionField subField : subscription.getFields()) {
            for (PublicationField pubField : publication.getFields()) {
                if (subField.getFieldName().equals(pubField.getFieldName()) && subField.getValue().equals(pubField.getValue().toString())) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("notification-stream", new Fields("subscriberId", "publication"));
    }
}