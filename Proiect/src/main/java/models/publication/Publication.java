package models.publication;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Publication {
    private final List<PublicationField> fields;

    public Publication() {
        this.fields = Collections.synchronizedList(new ArrayList<>());
    }

    public void addField(PublicationField field) {
        synchronized (fields) {
            fields.add(field);
        }
    }

    public List<PublicationField> getFields() {
        return fields;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        for (PublicationField field : fields) {
            sb.append(field.toString());
            sb.append("; ");
        }
        sb.delete(sb.length() - 2, sb.length());
        sb.append("}");
        return sb.toString();
    }
}
