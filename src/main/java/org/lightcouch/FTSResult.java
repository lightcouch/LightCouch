package org.lightcouch;

import java.util.List;
import com.google.gson.annotations.SerializedName;

public class FTSResult {
    @SerializedName("q")
    private String query;
    @SerializedName("total_rows")
    private int totalRows;
    private int skip;
    private int limit;
    private List<FTSResultEntry> rows;

    public String getQuery() {
        return query;
    }

    public int getTotalRows() {
        return totalRows;
    }

    public int getSkip() {
        return skip;
    }

    public int getLimit() {
        return limit;
    }

    public List<FTSResultEntry> getRows() {
        return rows;
    }
}
