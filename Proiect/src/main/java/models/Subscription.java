package models;

import org.json.JSONObject;
import org.json.JSONException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Subscription {

    private Map<String, String> info = new HashMap<>();
    private List<String> operator = new ArrayList<>();

    public Subscription() {
    }

    public Map<String, String> getInfo() {
        return info;
    }

    public void addInfo(String metaInfo, String value) {
        this.info.put(metaInfo, value);
    }

    public void addOperator(String operator)
    {
        this.operator.add(operator);
    }

    public List<String> getOperator() {
        return operator;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Subscription{");
        int i = 0;
        for (Map.Entry<String, String> entry : info.entrySet()) {
            sb.append("(").append(entry.getKey()).append(",").append(operator.get(i)).append(",").append(entry.getValue()).append(");");
            i++;
        }
        sb.append("}\n");
        return sb.toString();
    }

    public boolean matches(String publication) {
        try {
            JSONObject json = new JSONObject(publication);
            int i = 0;
            for (Map.Entry<String, String> entry : info.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();
                String op = operator.get(i);

                int pubValue = json.getInt(key);

                switch (op) {
                    case "=":
                        if (pubValue != Integer.parseInt(value)) return false;
                        break;
                    case ">":
                        if (pubValue <= Integer.parseInt(value)) return false;
                        break;
                    case "<":
                        if (pubValue >= Integer.parseInt(value)) return false;
                        break;
                    case ">=":
                        if (pubValue < Integer.parseInt(value)) return false;
                        break;
                    case "<=":
                        if (pubValue > Integer.parseInt(value)) return false;
                        break;
                    default:
                        return false;
                }
                i++;
            }
            return true;
        } catch (JSONException e) {
            e.printStackTrace();
            return false;
        }
    }
}
