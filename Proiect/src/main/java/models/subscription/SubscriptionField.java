package models.subscription;


import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class SubscriptionField implements Serializable {
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


    public static SimpleDateFormat getDateFormat() {
        return dateFormat;
    }

}
