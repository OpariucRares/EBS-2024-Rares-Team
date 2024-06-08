package storm;

import models.publication.Publication;
import models.subscription.Subscription;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
        for (Subscription subscription : subscriptions) {
            collector.emit("subscription-stream", new Values(subscriberId, subscription.toString()));
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
        declarer.declareStream("subscription-stream", new Fields("subscriberId", "subscription"));    }
}