package models;

import java.util.HashMap;
import java.util.Map;

public class Subscription {

    private Map<String, String> info = new HashMap<>();


    public Subscription() {
    }

    public Map<String, String> getInfo() {
        return info;
    }

    public void addInfo(String metaInfo, String value) {
        this.info.put(metaInfo, value);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Subscription{info=").append(info).append("}\n");
        return sb.toString();
    }



}
