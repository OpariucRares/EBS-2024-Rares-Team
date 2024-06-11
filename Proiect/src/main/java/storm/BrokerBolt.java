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

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

public class BrokerBolt extends BaseRichBolt {
    private OutputCollector collector;
    private Map<String, List<Map<String, Map<Object, String>>>> subscriptionMap;
    private String brokerId;
    private ZooKeeper zkClient;
    private static final String ZK_BROKER_PATH = "/zookeeper";
    private int receivedPublicationsNumber;
    private int matchedPublicationsNumber;

    public BrokerBolt(String brokerId) {
        this.subscriptionMap = Collections.synchronizedMap(new HashMap<>());
        this.brokerId = brokerId;
        this.receivedPublicationsNumber = 0;
        this.matchedPublicationsNumber = 0;
    }

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        
        try {
            zkClient = new ZooKeeper("localhost:2181", 3000, null); // Eliminați watcher-ul din constructor

            String brokerZnodePath = ZK_BROKER_PATH + "/" + brokerId;
            Stat stat = zkClient.exists(brokerZnodePath, false);
            if (stat == null) {
                zkClient.create(brokerZnodePath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }

            zkClient.exists(brokerZnodePath, watchedEvent -> {
                if (watchedEvent.getType() == Watcher.Event.EventType.NodeDeleted) {
                    //handleBrokerDown(brokerZnodePath)
                }
            });

        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple tuple) {
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
//                System.out.println("Added to subMap! Current map size: " + subscriptionMap.size());
//                System.out.println("Current subscriptionMap: " + subscriptionMap);
            }

            collector.emit("subscription-stream", new Values(brokerId, company, value, drop, variation, date));


        } else if ("notification-stream".equals(streamId) || "default".equals(streamId)) {
            receivedPublicationsNumber++;
            System.out.println("Processing publication...");

            long emissionTime = tuple.getLongByField("emissionTime");

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

//            System.out.println("SubscriptionMap before processing publication: " + subscriptionMap);
            synchronized (subscriptionMap) {
                for (Map.Entry<String, List<Map<String, Map<Object, String>>>> entry : subscriptionMap.entrySet()) {
                    boolean isFound = false;
                    String subscriberId = entry.getKey();
                    List<Map<String, Map<Object, String>>> subscriptions = entry.getValue();
                    for (Map<String, Map<Object, String>> subscription : subscriptions) {
                        if (matches(subscription, publication)) {
                            collector.emit("notification-stream",
                                    new Values(subscriberId, company, value, drop, variation, date, emissionTime));
                            matchedPublicationsNumber++;
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
                new Fields("subscriberId", "company", "value", "drop", "variation", "date", "emissionTime"));
        declarer.declareStream("subscription-stream",
                new Fields("subscriberId", "company", "value", "drop", "variation", "date"));
    }

    @Override
    public void cleanup() {
        try (BufferedWriter writer = new BufferedWriter(
                new FileWriter("results/stats/" + brokerId + ".txt"))) {
            writer.write("Publications received: " + receivedPublicationsNumber +
                    "\nPublications matched: " + matchedPublicationsNumber);
            writer.newLine();
        } catch (IOException e) {
            System.err.println("Error writing to file: " + e.getMessage());
        }
    }
    //TENTATIVA PENTRU CADEREA BROKERILOR
//    private void handleBrokerDown(String path) {
//        //    String downBrokerId = path.substring(path.lastIndexOf('/') + 1);
////
////        System.out.println("Broker down detected: " + downBrokerId);
////    List<String> activeBrokers = getActiveBrokers();
////
////        for (String message : getMessagesForBroker(downBrokerId)) {
////        String newBrokerId = chooseNewBroker(activeBrokers);
////        rerouteMessage(message, downBrokerId, newBrokerId);
////    }
////
////    // 4. Actualizați metadatele sistemului pentru a reflecta schimbarea
////    updateSystemMetadata(downBrokerId, activeBrokers);
//    }
//
//    private List<String> getActiveBrokers() {
//        List<String> activeBrokers = new ArrayList<>();
//        try {
//            // Obțineți toate znode-urile din calea brokerilor
//            List<String> brokerNodes = zkClient.getChildren(ZK_BROKER_PATH, false);
//
//            // Adăugați toți brokerii activi în listă
//            for (String brokerNode : brokerNodes) {
//                String brokerPath = ZK_BROKER_PATH + "/" + brokerNode;
//                if (zkClient.exists(brokerPath, false) != null) {
//                    activeBrokers.add(brokerNode);
//                }
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        return activeBrokers;
//    }
//    private List<String> getMessagesForBroker(String brokerId) {
//        return new ArrayList<>();
//    }
//    private String chooseNewBroker(List<String> activeBrokers) {
//        if (activeBrokers == null || activeBrokers.isEmpty()) {
//            throw new IllegalStateException("Nu există brokeri activi disponibili.");
//        }
//
//        Random random = new Random();
//        int brokerIndex = random.nextInt(activeBrokers.size());
//        return activeBrokers.get(brokerIndex);
//    }
//
//    private void rerouteMessage(String message, String downBrokerId, String newBrokerId) {
//        //algorithm
//    }
//
//    private List<String> getTasksForBroker(String brokerId) {
//        return new ArrayList<>();
//    }
//
//    private void reassignTask(String task, String newBrokerId) {
//    }
//
//    private void updateSystemMetadata(String downBrokerId, List<String> activeBrokers) {
//        // ...
//    }
}