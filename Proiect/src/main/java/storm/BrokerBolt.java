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
import org.json.JSONArray;
import org.json.JSONObject;

import java.text.ParseException;
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
//        String subscriptionsJson = (String) topoConf.get("subscriptions");
//
//        // Null check
//        if (subscriptionsJson == null) {
//            throw new RuntimeException("subscriptionsJson is null");
//        }
//
//        JSONArray jsonArray = new JSONArray(subscriptionsJson);
//        for (int i = 0; i < jsonArray.length(); i++) {
//            JSONObject jsonObject = jsonArray.getJSONObject(i);
//            JSONArray fieldsArray = jsonObject.getJSONArray("fields");
//            List<SubscriptionField> fields = new ArrayList<>();
//            for (int j = 0; j < fieldsArray.length(); j++) {
//                JSONObject fieldObject = fieldsArray.getJSONObject(j);
//                String fieldName = fieldObject.getString("fieldName");
//                String operator = fieldObject.getString("operator");
//                String valueType = fieldObject.getString("valueType");
//                Object value;
//
//                try {
//                    if ("Date".equals(valueType)) {
//                        value = SubscriptionField.getDateFormat().parse(fieldObject.getString("value"));
//                    } else if ("Integer".equals(valueType)) {
//                        value = Integer.parseInt(fieldObject.getString("value"));
//                    } else if ("Double".equals(valueType)) {
//                        value = Double.parseDouble(fieldObject.getString("value"));
//                    } else {
//                        value = fieldObject.getString("value");
//                    }
//                } catch (ParseException e) {
//                    throw new RuntimeException("Error parsing date field", e);
//                }
//
//                SubscriptionField field = new SubscriptionField(fieldName, operator, value);
//                fields.add(field);
//            }
//            Subscription subscription = new Subscription(fields);
//            this.subscriptions.add(subscription);
//        }
    }

    @Override
    public void execute(Tuple tuple) {
        String sourceComponent = tuple.getSourceComponent();
        if (sourceComponent.equals("subscriber")) {
            //logica de preluare a valorilor de la spout-ul subscriber
            //String subscriberId = input.getStringByField("subscriberId");
            //SubscriptionDEPRECATED subscriptionDEPRECATED = (SubscriptionDEPRECATED) input.getValueByField("subscription");
            //doar il adauga
        }
        else if (sourceComponent.equals("publisher")){
            //cauti in lista de subscriberi -> gasesti, faci match
            //String publication = input.getStringByField("publication");
            //            for (String subscriber : subscribers) {
            //                for (SubscriptionDEPRECATED subscriptionDEPRECATED : subscriptionsMap.get(subscriber)) {
            //                    if (subscriptionDEPRECATED.matches(publication)) {
            //                        collector.emit(new Values(subscriber, publication));
            //                    }
            //                }
            //            }
        }
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
        declarer.declare(new Fields("publication"));
    }
}