package org.lightcouch;

public class FTSQuery {
    private String designName;
    private String indexName;
    private String query;

    public FTSQuery(String designName, String indexName, String query) {
        this.designName = designName;
        this.indexName = indexName;
        this.query = query;
    }

    public String getDesignName() {
        return designName;
    }

    public String getIndexName() {
        return indexName;
    }

    public String getQuery() {
        return query;
    }
}
