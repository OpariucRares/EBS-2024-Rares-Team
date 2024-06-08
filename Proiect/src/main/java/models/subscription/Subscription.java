package models.subscription;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Subscription implements Serializable {
    private final List<SubscriptionField> fields;

    public Subscription() {
        this.fields = Collections.synchronizedList(new ArrayList<>());
    }

    public Subscription(List<SubscriptionField> fields) {
        this.fields = Collections.synchronizedList(new ArrayList<>(fields));
    }

    public void addField(SubscriptionField field) {
        synchronized (fields) {
            fields.add(field);
        }
    }

    public List<SubscriptionField> getFields() {
        return fields;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        for (SubscriptionField field : fields) {
            sb.append(field.toString());
            sb.append("; ");
        }
        sb.delete(sb.length() - 2, sb.length());
        sb.append("}");
        return sb.toString();
    }
}
