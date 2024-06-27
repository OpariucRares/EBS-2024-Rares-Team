package storm;

import com.google.protobuf.InvalidProtocolBufferException;
import models.publication.PublicationOuterClass;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class SecondaryBolt extends BaseRichBolt {
    private OutputCollector collector;

    private Map<String, List<Map<String, Map<Object, String>>>> subscriptionMap;
    private String brokerId;
    private int receivedPublicationsNumber;
    private int matchedPublicationsNumber;

    private final AtomicBoolean isPrimaryActive = new AtomicBoolean(true);
    private final AtomicInteger missedHeartbeats = new AtomicInteger(0);
    private final int MAX_MISSED_HEARTBEATS = 5;
    private final long HEARTBEAT_INTERVAL = 3000; // 1 second for heartbeat interval

    private static final long TIMEOUT = 15000;
    private volatile long lastHeartbeatTime;

    public SecondaryBolt(String brokerId) {
        this.subscriptionMap = Collections.synchronizedMap(new HashMap<>());
        this.brokerId = brokerId;
        this.receivedPublicationsNumber = 0;
        this.matchedPublicationsNumber = 0;
    }

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;

        // Start a thread to check heartbeats
        new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(HEARTBEAT_INTERVAL); // Check heartbeat every second
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                if (this.isPrimaryActive.get()) {
                    int missed = this.missedHeartbeats.incrementAndGet();
                    if (missed >= this.MAX_MISSED_HEARTBEATS) {
                        this.isPrimaryActive.set(false);
                        System.err.println("Primary broker bolt missed heartbeats. Secondary broker bolt will start emitting.");
                    }
                }
            }
        }).start();
//        new Thread(() -> {
//            while (true) {
//                try {
//                    Thread.sleep(HEARTBEAT_INTERVAL);
//                } catch (InterruptedException e) {
//                    Thread.currentThread().interrupt();
//                }
//                if (this.isPrimaryActive.get()) {
//                    int missed = this.missedHeartbeats.incrementAndGet();
//                    if (missed >= this.MAX_MISSED_HEARTBEATS) {
//                        this.isPrimaryActive.set(false);
//                        System.err.println("Primary broker bolt missed heartbeats. Secondary broker bolt will start emitting.");
//                    }
//                }
//            }
//        }).start();
    }

    @Override
    public void execute(Tuple tuple) {
        String streamId = tuple.getSourceStreamId();

        if ("heartbeat-stream".equals(streamId)) {
            System.err.println(tuple.getValueByField("brokerId") + " sent heartbeat. Primary is alive !");
            this.missedHeartbeats.set(0);
            lastHeartbeatTime = System.currentTimeMillis();
        }
        else if (!isPrimaryActive.get()) {
            System.err.println("Primary broker is dead! Emitting...");
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

                        PublicationOuterClass.Publication publication = PublicationOuterClass.Publication.parseFrom(serializedPublication);
                        // Process the deserialized publication
                        for (PublicationOuterClass.PublicationField field : publication.getFieldsList()) {
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
        declarer.declareStream("heartbeat-stream", new Fields("heartbeat"));
    }
}
