package models.publication;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;

public class PublicationField implements Serializable {
    private String fieldName;
    private Object value;

    private static SimpleDateFormat dateFormat = new SimpleDateFormat("dd.MM.yyyy");

    public PublicationField(String fieldName, Object value) {
        this.fieldName = fieldName;
        this.value = value;
    }

    public String getFieldName() {
        return fieldName;
    }

    public Object getValue() {
        return value;
    }

    public String toString() {
        if (value instanceof String)
            return "(" + fieldName + ", \"" + value + "\")";
        else if(value instanceof Date)
            return "(" + fieldName + ", " + dateFormat.format(value) + ")";
        return "(" + fieldName + ", " + value + ")";
    }
}
