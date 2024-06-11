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

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

public class SubscriberBolt extends BaseRichBolt {
    private OutputCollector collector;
    private List<Subscription> subscriptions;
    private String subscriberId;
    private long totalLatency;
    private int receivedPublicationsNumber;

    public SubscriberBolt(String subscriberId, List<Subscription> subscriptions) {
        this.subscriberId = subscriberId;
        this.subscriptions = subscriptions;
        this.totalLatency = 0L;
        this.receivedPublicationsNumber = 0;
    }

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;

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
        String streamId = tuple.getSourceStreamId();
        if ("notification-stream".equals(streamId)) {
            receivedPublicationsNumber++;

            long receiveTime = System.currentTimeMillis();
            long emissionTime = tuple.getLongByField("emissionTime");
            totalLatency += (receiveTime- emissionTime);

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

            System.out.println(tuple.getStringByField("subscriberId") + "Yey I got a pub ! " + publication.toString());

            try (BufferedWriter writer = new BufferedWriter(
                    new FileWriter("results/publication-received-by-" + tuple.getStringByField("subscriberId") + ".txt",
                            true))) {
                writer.write(publication.toString() + " { Emission time: " + emissionTime + "} | { Receive time: " +
                        receiveTime + " } | { Transmission time: " + (receiveTime - emissionTime) + " milliseconds }");
                writer.newLine();
            } catch (IOException e) {
                System.err.println("Error writing to file: " + e.getMessage());
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("subscription-stream",
                new Fields("subscriberId", "company", "value", "drop", "variation", "date"));
    }

    @Override
    public void cleanup() {
        if (receivedPublicationsNumber > 0) {
            try (BufferedWriter writer = new BufferedWriter(
                    new FileWriter("results/stats/" + subscriberId + ".txt"))) {
                writer.write("Publications received: " + receivedPublicationsNumber +
                        "\nMean latency:" + (totalLatency / receivedPublicationsNumber) + " milliseconds");
                writer.newLine();
            } catch (IOException e) {
                System.err.println("Error writing to file: " + e.getMessage());
            }
        }
    }
}