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
import util.CoordinationSignal;

import java.util.*;

public class SubscriberBolt extends BaseRichBolt {
    private OutputCollector collector;
    private List<Subscription> subscriptions;
    private String subscriberId;

    public SubscriberBolt(String subscriberId, List<Subscription> subscriptions) {
        this.subscriberId = subscriberId;
        this.subscriptions = subscriptions;
    }

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;

        // Emiterea subscripțiilor către BrokerBolt la inițializare
        // TODO: DE FACUT TRANSMITEREA CORECTA. AICI AM TRIMIS CA STRING CA SA VAD CA MERGE
//        for (Subscription subscription : subscriptions) {
//            collector.emit("subscription-stream", new Values(subscriberId, subscription.toString()));
//        }
        for (Subscription subscription : subscriptions) {
            Map<String, String> company = new HashMap<>();
            Map<Double, String> value = new HashMap<>();
            Map<Double, String> drop = new HashMap<>();
            Map<Double, String> variation = new HashMap<>();
            Map<Date, String> date = new HashMap<>();

            for (SubscriptionField field : subscription.getFields()) {
                switch (field.getFieldName()) {
                    case "company":
                        company.put((String) field.getValue(), field.getOperator());
                        break;
                    case "value":
                        value.put((double) field.getValue(), field.getOperator());
                        break;
                    case "drop":
                        drop.put((double) field.getValue(),  field.getOperator());
                        break;
                    case "variation":
                        variation.put((double) field.getValue(), field.getOperator());
                        break;
                    case "date":
                        date.put((Date) field.getValue(), field.getOperator());
                        break;
                }
            }
            collector.emit("subscription-stream",
                    new Values(subscriberId, company, value, drop, variation, date));
        }
    }

    @Override
    public void execute(Tuple tuple) {

        // Procesarea notificărilor de la BrokerBolt
        if (tuple.getSourceComponent().equals("broker-bolt")) {
            // Presupunem că notificarea este primită ca un obiect serializat
            // și că include subscriberId pentru a verifica destinatarul
            System.out.println("Am primit ceva de la bolt");
            String notificationSubscriberId = tuple.getStringByField("subscriberId");
            // TODO: DE ASTA AVEM NEVOIE DE SUBSCRIBER ID, CA SA STIM CINE A CERUT
            if (this.subscriberId.equals(notificationSubscriberId)) {
                // Logica de procesare a notificării
                // ...
                System.out.println("eu sunt subscriber bolt cu id-ul" + subscriberId);
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // Nu este necesar să declarăm câmpuri de ieșire dacă acest bolt nu emite tuple mai departe
        // TODO: DE MODIFICAT CUM ESTE SUS
//        declarer.declareStream("subscription-stream", new Fields("subscriberId", "subscription"));
        declarer.declareStream("subscription-stream",
                new Fields("subscriberId", "company", "value", "drop", "variation", "date"));
    }
}