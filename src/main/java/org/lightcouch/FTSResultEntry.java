package org.lightcouch;

import java.util.Map;

public class FTSResultEntry {
    private String id;
    private String score;
    private Map<String, Object> fields;

    public String getId() {
        return id;
    }

    public String getScore() {
        return score;
    }

    public Map<String, Object> getFields() {
        return fields;
    }

    public String getSummary() {
        if (fields != null)
            return (String) fields.get("summary");
        return null;
    }
}
