package org.example;

import java.util.HashMap;
import java.util.Map;

public class Subscription {

    private Map<String, String> info = new HashMap();

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
        return "Subscription{" +
                "info=" + info +
                '}';
    }



}
