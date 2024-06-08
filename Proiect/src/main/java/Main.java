import models.publication.Publication;
import models.publication.PublicationField;
import models.subscription.Subscription;
import models.subscription.SubscriptionField;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import models.publication.PublicationGenerator;
import models.subscription.SubscriptionGenerator;
import storm.BrokerBolt;
import storm.PublisherSpout;
import storm.SubscriberBolt;
import util.Constants;
import java.util.Arrays;
import java.util.Date;

import org.json.JSONArray;
import org.json.JSONObject;

public class Main {
    public static void main(String[] args) throws Exception {
        Constants constants = new Constants();

        SubscriptionGenerator subscriptionGenerator = new SubscriptionGenerator();
        var subscriptions = subscriptionGenerator.generateSubscriptions(10, constants.fieldFreq, constants.eqFreq);

//        // Serialize subscriptions to JSON
//        JSONArray subscriptionsJson = new JSONArray();
//        for (Subscription subscription : subscriptions) {
//            JSONObject subscriptionJson = new JSONObject();
//            JSONArray fieldsJson = new JSONArray();
//            for (SubscriptionField field : subscription.getFields()) {
//                JSONObject fieldJson = new JSONObject();
//                fieldJson.put("name", field.getFieldName());
//                fieldJson.put("value", field.getValue());
//                fieldsJson.put(fieldJson);
//            }
//            subscriptionJson.put("fields", fieldsJson);
//            subscriptionsJson.put(subscriptionJson);
//        }

        // Serialize subscriptions to JSON
        JSONArray subscriptionsJson = new JSONArray();
        for (Subscription subscription : subscriptions) {
            JSONObject subscriptionJson = new JSONObject();
            JSONArray fieldsJson = new JSONArray();
            for (SubscriptionField field : subscription.getFields()) {
                JSONObject fieldJson = new JSONObject();
                fieldJson.put("fieldName", field.getFieldName());
                fieldJson.put("operator", field.getOperator());
                if (field.getValue() instanceof Date) {
                    fieldJson.put("value", SubscriptionField.getDateFormat().format(field.getValue()));
                    fieldJson.put("valueType", "Date");
                } else {
                    fieldJson.put("value", field.getValue().toString());
                    fieldJson.put("valueType", field.getValue().getClass().getSimpleName());
                }
                fieldsJson.put(fieldJson);
            }
            subscriptionJson.put("fields", fieldsJson);
            subscriptionsJson.put(subscriptionJson);
        }

        PublicationGenerator publicationGenerator = new PublicationGenerator();
        var publications = publicationGenerator.generatePublications(10, constants.pubFieldFreq);
//        var publications2 = publicationGenerator.generatePublications(100000, constants.pubFieldFreq);

        // Serialize publications to JSON
        JSONArray publicationsJson = new JSONArray();
        for (Publication publication : publications) {
            JSONObject publicationJson = new JSONObject();
            JSONArray fieldsJson = new JSONArray();
            for (PublicationField field : publication.getFields()) {
                JSONObject fieldJson = new JSONObject();
                fieldJson.put("name", field.getFieldName());
                fieldJson.put("value", field.getValue());
                fieldsJson.put(fieldJson);
            }
            publicationJson.put("fields", fieldsJson);
            publicationsJson.put(publicationJson);
        }

        // One PublisherSpout instance might mean duplicated values
        PublisherSpout publisherSpout = new PublisherSpout();
        BrokerBolt brokerBolt = new BrokerBolt();
        SubscriberBolt subscriberBolt = new SubscriberBolt();

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("publisher-spout", publisherSpout, 2);
        builder.setBolt("broker-bolt", brokerBolt, 3).shuffleGrouping("publisher-spout");
        builder.setBolt("subscriber-bolt", subscriberBolt, 3).shuffleGrouping("broker-bolt");

        // Config
        Config config = new Config();
        config.setDebug(true);

        config.put("subscriptions", subscriptionsJson.toString());
        config.put("publications", publicationsJson.toString());
        config.setNumWorkers(3);

        config.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList("localhost"));
        config.put(Config.STORM_ZOOKEEPER_PORT, 2181);

        if (args.length == 0) {
            // Run the topology in a local cluster
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("word-count-topology", config, builder.createTopology());

            // Keep the topology running for some time (e.g., 60 seconds) for demonstration purposes
            Thread.sleep(6000);

            // Shutdown the local cluster
            cluster.shutdown();
        } else {
            // Submit the topology to the Storm cluster
            StormSubmitter.submitTopology(args[0], config, builder.createTopology());
        }
    }
}