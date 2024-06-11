package storm;

import models.publication.Publication;
import models.publication.PublicationField;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

//import org.apache.zookeeper.*;
//import org.apache.zookeeper.data.Stat;

import java.util.*;

public class RerouteBolt extends BaseRichBolt {
    private OutputCollector collector;
    private Map<String, List<Map<String, Map<Object, String>>>> subscriptionMap;
    private String brokerId;
    //private ZooKeeper zkClient;
    private static final String ZK_BROKER_PATH = "/zookeeper";
    private boolean isBackupMode = false;

    public RerouteBolt(String brokerId) {
        this.subscriptionMap = Collections.synchronizedMap(new HashMap<>());
        this.brokerId = brokerId;
    }
    public void activateBackupMode() {
        isBackupMode = true;
    }

    public void deactivateBackupMode() {
        isBackupMode = false;
    }

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;

//        try {
//            zkClient = new ZooKeeper("localhost:2181", 3000, watchedEvent -> {});
//
//            String brokerZnodePath = ZK_BROKER_PATH + "/" + brokerId;
//            Stat stat = zkClient.exists(brokerZnodePath, true);
//            if (stat == null) {
//                zkClient.create(brokerZnodePath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
    }

    @Override
    public void execute(Tuple tuple) {
        if (!isBackupMode) return;

        String streamId = tuple.getSourceStreamId();

        if ("subscription-stream".equals(streamId)) {

            String subscriberId = tuple.getStringByField("subscriberId");

            Map<String, String> company = (Map<String, String>) tuple.getValueByField("company");
            Map<Double, String> value = (Map<Double, String>) tuple.getValueByField("value");
            Map<Double, String> drop = (Map<Double, String>) tuple.getValueByField("drop");
            Map<Double, String> variation = (Map<Double, String>) tuple.getValueByField("variation");
            Map<Date, String> date = (Map<Date, String>) tuple.getValueByField("date");

            // Create a list of maps representing the subscription fields
            Map<String, Map<Object, String>> subscriptionFields = new HashMap<>();
            subscriptionFields.put("company", (Map<Object, String>) (Map<?, ?>) company);
            subscriptionFields.put("value", (Map<Object, String>) (Map<?, ?>) value);
            subscriptionFields.put("drop", (Map<Object, String>) (Map<?, ?>) drop);
            subscriptionFields.put("variation", (Map<Object, String>) (Map<?, ?>) variation);
            subscriptionFields.put("date", (Map<Object, String>) (Map<?, ?>) date);

            synchronized (subscriptionMap) {
                subscriptionMap.computeIfAbsent(subscriberId, k -> new ArrayList<>()).add(subscriptionFields);
                System.out.println("Added to subMap! Current map size: " + subscriptionMap.size());
                System.out.println("Current subscriptionMap: " + subscriptionMap);
            }

            collector.emit("subscription-stream", new Values(brokerId, company, value, drop, variation, date));


        } else if ("notification-stream".equals(streamId) || "default".equals(streamId)) {
            System.out.println("Processing publication...");
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

            System.out.println("SubscriptionMap before processing publication: " + subscriptionMap);
            synchronized (subscriptionMap) {
                for (Map.Entry<String, List<Map<String, Map<Object, String>>>> entry : subscriptionMap.entrySet()) {
                    boolean isFound = false;
                    String subscriberId = entry.getKey();
                    List<Map<String, Map<Object, String>>> subscriptions = entry.getValue();
                    for (Map<String, Map<Object, String>> subscription : subscriptions) {
                        if (matches(subscription, publication)) {
                            collector.emit("notification-stream",
                                    new Values(subscriberId, company, value, drop, variation, date));
                            isFound = true;
                            break;
                        }
                    }
                    if (isFound) {
                        break;
                    }
                }
            }
        }

    }

    private boolean matches(Map<String, Map<Object, String>> subscription, Publication publication) {
        for (PublicationField pubField : publication.getFields()) {
            String pubFieldName = pubField.getFieldName();
            Object pubFieldValue = pubField.getValue();

            Map<Object, String> subFieldMap = subscription.get(pubFieldName);
            if (!subFieldMap.isEmpty()) {
                for (Map.Entry<Object, String> entry : subFieldMap.entrySet()) {
                    Object subFieldValue = entry.getKey();
                    String operator = entry.getValue();

                    switch (pubFieldName) {
                        case "company":
                            return compareStrings((String) pubFieldValue, (String) subFieldValue, operator);
                        // TODO: verify date
                        case "date":
                            return compareDates((Date) pubFieldValue, (Date) subFieldValue, operator);
                        case "value":
                        case "drop":
                        case "variation":
                            return compareDoubles((Double) pubFieldValue, (Double) subFieldValue, operator);
                        default:
                            throw new IllegalArgumentException("Unsupported field name: " + pubFieldName);
                    }
                }
                return false;
            }

        }
        return false;
    }

    public static boolean compareStrings(String pubFieldValue, String subFieldValue, String operator) {
        switch (operator) {
            case "=":
                return pubFieldValue.equals(subFieldValue);
            default:
                return false;
        }
    }

    public static boolean compareDates(Date pubFieldValue, Date subFieldValue, String operator) {
        switch (operator) {
            case "=":
                return pubFieldValue.equals(subFieldValue);
            case "<":
                return pubFieldValue.before(subFieldValue);
            case ">":
                return pubFieldValue.after(subFieldValue);
            default:
                return false;
        }
    }

    public static boolean compareDoubles(Double pubFieldValue, Double subFieldValue, String operator) {
        switch (operator) {
            case "=":
                return pubFieldValue.equals(subFieldValue);
            case "<":
                return pubFieldValue < subFieldValue;
            case ">":
                return pubFieldValue > subFieldValue;
            default:
                return false;
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("notification-stream",
                new Fields("subscriberId", "company", "value", "drop", "variation", "date"));
        declarer.declareStream("subscription-stream",
                new Fields("subscriberId", "company", "value", "drop", "variation", "date"));
    }
    public void watchForActivationCommands() {
        try {
            // Înregistrați un watcher pentru a primi notificări despre schimbările de date ale znode-ului
//            zkClient.getData(ZK_BROKER_PATH, new Watcher() {
//                @Override
//                public void process(WatchedEvent watchedEvent) {
//                    if (watchedEvent.getType() == Watcher.Event.EventType.NodeDataChanged) {
//                        // Când datele znode-ului se schimbă, preluați noile date
//                        try {
//                            byte[] newData = zkClient.getData(ZK_BROKER_PATH, false, null);
//                            if (new String(newData).equals("activate")) {
//                                isBackupMode = true;
//                            }
//                        } catch (Exception e) {
//                            e.printStackTrace();
//                        }
//                    }
//                }
//            }, null);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}