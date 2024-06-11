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

import java.util.*;

public class BrokerBolt extends BaseRichBolt {
    private OutputCollector collector;
    private Map<String, List<Map<String, Map<Object, String>>>> subscriptionMap;
    private String brokerId;


    public BrokerBolt(String brokerId) {
        this.subscriptionMap = Collections.synchronizedMap(new HashMap<>());
        this.brokerId = brokerId;
    }

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
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
                System.out.println("Added to subMap! Current map size: " + subscriptionMap.size());
                System.out.println("Current subscriptionMap: " + subscriptionMap);
            }

            collector.emit("subscription-stream", new Values(brokerId, company, value, drop, variation, date));


        } else if ("notification-stream".equals(streamId) || "default".equals(streamId) || "decoded-stream".equals(streamId)) {
            System.out.println("Processing publication...");
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
                        System.out.println("Field Name: " + field.getFieldName());
                        switch (field.getValueCase()) {
                            case COMPANYFIELD:
                                System.out.println("Company Value: " + field.getCompanyField());
                                company = field.getCompanyField();
                                break;
                            case VALUEFIELD:
                                System.out.println("Value Value: " + field.getValueField());
                                value = field.getValueField();
                                break;
                            case DROPFIELD:
                                System.out.println("Drop Value: " + field.getDropField());
                                drop = field.getDropField();
                                break;
                            case VARIATIONFIELD:
                                System.out.println("Variation Value: " + field.getVariationField());
                                variation = field.getVariationField();
                                break;
                            case DATEFIELD:
                                System.out.println("Date Value: " + field.getDateField());
                                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
                                date = dateFormat.parse(field.getDateField());
                                break;
                            case VALUE_NOT_SET:
                                System.out.println("Value not set");
                                break;
                        }
                    }

                        collector.emit("decoded-stream",
                                new Values(company, value, drop, variation, date));

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


                System.out.println("SubscriptionMap before processing publication: " + subscriptionMap);
                synchronized (subscriptionMap) {
                    for (Map.Entry<String, List<Map<String, Map<Object, String>>>> entry : subscriptionMap.entrySet()) {
                        boolean isFound = false;
                        String subscriberId = entry.getKey();
                        List<Map<String, Map<Object, String>>> subscriptions = entry.getValue();
                        for (Map<String, Map<Object, String>> subscription : subscriptions) {
                            if (matches(subscription, publicationFields)) {
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

    }

    private boolean matches(Map<String, Map<Object, String>> subscription, Map<String, Object> publication) {
        for (Map.Entry<String, Object> pubEntry : publication.entrySet()) {
            String pubFieldName = pubEntry.getKey();
            Object pubFieldValue = pubEntry.getValue();

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
        declarer.declareStream("decoded-stream",
                new Fields("company", "value", "drop", "variation", "date"));
    }
}