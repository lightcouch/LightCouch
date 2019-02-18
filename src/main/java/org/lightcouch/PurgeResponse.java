package org.lightcouch;

import java.util.List;
import java.util.Map;

import com.google.gson.annotations.SerializedName;

public class PurgeResponse {

    @SerializedName("purge_seq")
    private String purgeSeq;
    
    public String getPurgeSeq() {
        return purgeSeq;
    }

    public void setPurgeSeq(String purgeSeq) {
        this.purgeSeq = purgeSeq;
    }

    private Map<String,List<String>> purged;

    public Map<String,List<String>> getPurged() {
        return purged;
    }

    public void setPurged(Map<String,List<String>> purged) {
        this.purged = purged;
    }
    
}
