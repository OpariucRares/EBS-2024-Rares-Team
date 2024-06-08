package models.subscription;

import org.json.JSONObject;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class SubscriptionField {
    private String fieldName;
    private String operator;
    private Object value;

    private static SimpleDateFormat dateFormat = new SimpleDateFormat("dd.MM.yyyy");

    public SubscriptionField(String fieldName, String operator, Object value) {
        this.fieldName = fieldName;
        this.operator = operator;
        this.value = value;
    }

    public String getFieldName() {
        return fieldName;
    }

    public String getOperator() {
        return operator;
    }

    public Object getValue() {
        return value;
    }

    public String toString() {
        if (value instanceof String)
            return "(" + fieldName + ", " + operator + ", \"" + value + "\")";
        else if(value instanceof Date)
            return "(" + fieldName + ", " + operator + ", " + dateFormat.format(value) + ")";
        return "(" + fieldName + ", " + operator + ", " + value + ")";
    }

    public static SubscriptionField fromJson(JSONObject jsonObject) throws ParseException {
        String fieldName = jsonObject.getString("fieldName");
        String operator = jsonObject.getString("operator");
        Object value = jsonObject.get("value");

        if (value instanceof String) {
            String strValue = (String) value;
            if (strValue.matches("\\d{2}\\.\\d{2}\\.\\d{4}")) {
                value = dateFormat.parse(strValue);
            }
        } else if (jsonObject.has("dateValue")) {
            value = dateFormat.parse(jsonObject.getString("dateValue"));
        }

        return new SubscriptionField(fieldName, operator, value);
    }

    public static SimpleDateFormat getDateFormat() {
        return dateFormat;
    }

    public JSONObject toJson() {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("fieldName", fieldName);
        jsonObject.put("operator", operator);
        if (value instanceof Date) {
            jsonObject.put("value", dateFormat.format((Date) value));
        } else {
            jsonObject.put("value", value);
        }
        return jsonObject;
    }
}
