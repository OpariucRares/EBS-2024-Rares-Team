package storm;

// import models.publication.Publication;
import com.google.protobuf.InvalidProtocolBufferException;
// import models.publication.PublicationField;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import models.publication.PublicationOuterClass.*;
import util.Utils;

//import org.apache.zookeeper.*;
//import org.apache.zookeeper.data.Stat;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class BrokerBolt extends BaseRichBolt {
    private OutputCollector collector;
    private Map<String, List<Map<String, Map<Object, String>>>> subscriptionMap;
    private String brokerId;

    private final AtomicBoolean isActive = new AtomicBoolean(true);
    private Thread heartbeatThread;
    private final long HEARTBEAT_INTERVAL = 3000;

    private final SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss.SSS");

    //private ZooKeeper zkClient;
    private static final String ZK_BROKER_PATH = "/zookeeper";
    private int receivedPublicationsNumber;
    private int matchedPublicationsNumber;

    public BrokerBolt(String brokerId) {
        this.subscriptionMap = Collections.synchronizedMap(new HashMap<>());
        this.brokerId = brokerId;
        this.receivedPublicationsNumber = 0;
        this.matchedPublicationsNumber = 0;
    }

    public String getBrokerId() {
        return this.brokerId;
    }

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;

        // Start a thread to emit heartbeats
        new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(HEARTBEAT_INTERVAL);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                if ("broker2".equals(this.brokerId)) {
                    collector.emit("heartbeat-stream", new Values("heartbeat", this.brokerId));
                }
            }
        }).start();

//        heartbeatThread = new Thread(() -> {
//            while (isActive.get() && !Thread.currentThread().isInterrupted()) {
//                try {
//                    Thread.sleep(HEARTBEAT_INTERVAL); // Emit heartbeat every second
//                    collector.emit("heartbeat-stream", new Values("heartbeat", this.brokerId));
//                } catch (InterruptedException e) {
//                    Thread.currentThread().interrupt();
//                }
//            }
//        });
//        heartbeatThread.start();
        
//        try {
//            zkClient = new ZooKeeper("localhost:2181", 3000, null); // Eliminați watcher-ul din constructor
//
//            String brokerZnodePath = ZK_BROKER_PATH + "/" + brokerId;
//            Stat stat = zkClient.exists(brokerZnodePath, false);
//            if (stat == null) {
//                zkClient.create(brokerZnodePath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
//            }
//
//            zkClient.exists(brokerZnodePath, watchedEvent -> {
//                if (watchedEvent.getType() == Watcher.Event.EventType.NodeDeleted) {
//                    //handleBrokerDown(brokerZnodePath)
//                }
//            });
//
//        } catch (KeeperException e) {
//            e.printStackTrace();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
    }

    public void simulateFailure() {
        isActive.set(false);
        if (heartbeatThread != null && heartbeatThread.isAlive()) {
            heartbeatThread.interrupt();
            try {
                heartbeatThread.join();  // Wait for the thread to finish
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        System.err.println("[" + dateFormat.format(new Date()) +
                "]" + this.brokerId + " bolt STARTED simulating failure and will not emit tuples.");
    }

    @Override
    public void execute(Tuple tuple) {
        if (!isActive.get()) {
//            System.err.println("[" + dateFormat.format(new Date()) +
//                    "]" + this.brokerId + " bolt is simulating failure and will not emit tuples.");
            return;
        }
        else if ("broker2".equals(this.brokerId)) {
//            System.err.println("[" + dateFormat.format(new Date()) +
//                    "]" + this.brokerId + " bolt FAILED simulating failure and will emit tuples." + isActive.get());
        }

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


        } else if ("notification-stream".equals(streamId) || "default".equals(streamId) || "decoded-stream".equals(streamId)) {
            if ("decoded-stream".equals(streamId))
                receivedPublicationsNumber++;

            long emissionTime = tuple.getLongByField("emissionTime");
            if("default".equals(streamId)) {
                byte[] serializedPublication = tuple.getBinaryByField("publication");
                try {

                    String company = null;
                    double value = 0.0;
                    double drop = 0.0;
                    double variation = 0.0;
                    Date date = null;

                    Publication publication = Publication.parseFrom(serializedPublication);
                    // Process the deserialized publication
                    for (PublicationField field : publication.getFieldsList()) {
//                        System.out.println("Field Name: " + field.getFieldName());
                        switch (field.getValueCase()) {
                            case COMPANYFIELD:
//                                System.out.println("Company Value: " + field.getCompanyField());
                                company = field.getCompanyField();
                                break;
                            case VALUEFIELD:
//                                System.out.println("Value Value: " + field.getValueField());
                                value = field.getValueField();
                                break;
                            case DROPFIELD:
//                                System.out.println("Drop Value: " + field.getDropField());
                                drop = field.getDropField();
                                break;
                            case VARIATIONFIELD:
//                                System.out.println("Variation Value: " + field.getVariationField());
                                variation = field.getVariationField();
                                break;
                            case DATEFIELD:
//                                System.out.println("Date Value: " + field.getDateField());
                                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
                                date = dateFormat.parse(field.getDateField());
                                break;
                            case VALUE_NOT_SET:
//                                System.out.println("Value not set");
                                break;
                        }
                    }

                    collector.emit("decoded-stream",
                                new Values(company, value, drop, variation, date, emissionTime));

                    } catch (InvalidProtocolBufferException e) {
                        e.printStackTrace();
                    } catch (ParseException e) {
                        throw new RuntimeException(e);
                    }
                }
            else {

    


            String company = tuple.getStringByField("company");
                double value = tuple.getDoubleByField("value");
                double drop = tuple.getDoubleByField("drop");
                double variation = tuple.getDoubleByField("variation");
                Date date = (Date) tuple.getValueByField("date");

                Map<String, Object> publicationFields = Map.of(
                        "company", company,
                        "value", value,
                        "drop", drop,
                        "variation", variation,
                        "date", date
                );


    //            System.out.println("SubscriptionMap before processing publication: " + subscriptionMap);
                synchronized (subscriptionMap) {
                    for (Map.Entry<String, List<Map<String, Map<Object, String>>>> entry : subscriptionMap.entrySet()) {
                        boolean isFound = false;
                        String subscriberId = entry.getKey();
                        List<Map<String, Map<Object, String>>> subscriptions = entry.getValue();
                        for (Map<String, Map<Object, String>> subscription : subscriptions) {
                            if (matches(subscription, publicationFields)) {
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

    }
    private boolean matches(Map<String, Map<Object, String>> subscription, Map<String, Object> publication) {
        for (Map.Entry<String, Map<Object, String>> subEntry : subscription.entrySet()) {
            String subFieldName = subEntry.getKey();
            Map<Object, String> subFieldMap = subEntry.getValue();

            Object pubFieldValue = publication.get(subFieldName);

            if (pubFieldValue != null && !subFieldMap.isEmpty()) {
                for (Map.Entry<Object, String> entry : subFieldMap.entrySet()) {
                    Object subFieldValue = entry.getKey();
                    String operator = entry.getValue();

                    switch (subFieldName) {
                        case "company":
                            if (!compareStrings((String) pubFieldValue, (String) subFieldValue, operator))
                                return false;
                            break;
                        case "date":
                            if (!compareDates((Date) pubFieldValue, (Date) subFieldValue, operator))
                                return false;
                            break;
                        case "value":
                        case "drop":
                        case "variation":
                            if (!compareDoubles((Double) pubFieldValue, (Double) subFieldValue, operator))
                                return false;
                            break;
                        default:
                            throw new IllegalArgumentException("Unsupported field name: " + subFieldName);
                    }
                }
            }
        }
        // all match
        return true;
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
        declarer.declareStream("decoded-stream",
                new Fields("company", "value", "drop", "variation", "date", "emissionTime"));
        declarer.declareStream("heartbeat-stream", new Fields("heartbeat", "brokerId"));
    }

    @Override
    public void cleanup() {
        if (!"broker1".equals(this.brokerId))
            return;

        try (BufferedWriter writer = new BufferedWriter(
                new FileWriter("results/stats/" + this.brokerId + ".txt"))) {
            writer.write("Publications received: " + receivedPublicationsNumber);
            writer.newLine();
        } catch (IOException e) {
            System.err.println("Error writing to file: " + e.getMessage());
        }

        // Ensure the heartbeat thread is stopped when the bolt is cleaned up
        isActive.set(false);
        if (heartbeatThread != null && heartbeatThread.isAlive()) {
            heartbeatThread.interrupt();
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