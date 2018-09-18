/*
 * Copyright (C) 2011 lightcouch.org Copyright (C) 2018 indaba.es
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.lightcouch;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.List;

import org.apache.commons.codec.Charsets;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.lightcouch.ChangesResult.Row;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

/**
 * <p>
 * Contains the Change Notifications API, supports <i>normal</i> and <i>continuous</i> feed Changes.
 * <h3>Usage Example:</h3>
 * 
 * <pre>
 * // feed type normal 
 * String since = dbClient.context().info().getUpdateSeq(); // latest update seq
 * ChangesResult changeResult = dbClient.changes()
 *	.since(since) 
 *	.limit(10)
 *	.filter("example/filter")
 *	.getChanges();
 *
 * for (ChangesResult.Row row : changeResult.getResults()) {
 *   String docId = row.getId()
 *   JsonObject doc = row.getDoc();
 * }
 *
 * // feed type continuous
 * Changes changes = dbClient.changes()
 *	.includeDocs(true) 
 *	.heartBeat(30000)
 *	.continuousChanges(); 
 * 
 * while (changes.hasNext()) { 
 *	ChangesResult.Row feed = changes.next();
 *  String docId = feed.getId();
 *  JsonObject doc = feed.getDoc();
 *	// changes.stop(); // stop continuous feed
 * }
 * 
 * Selector filter:
 * ChangesResult changeResult = dbClient.changes()
 *	.since(since) 
 *	.limit(10)
 *	.selector("{\"selector":{\"_deleted\":true}}")
 *	.getChanges();
 *
 * </pre>
 * 
 * @see ChangesResult
 * @since 0.0.2
 * @author Ahmed Yehia
 */
public class Changes {

    private BufferedReader reader;
    private HttpUriRequest httpRequest;
    private Row nextRow;
    private boolean stop;

    private CouchDbClientBase dbc;
    private Gson gson;
    private URIBuilder uriBuilder;

    private String filter;
    private String selector;
    private List<String> docIds;

    Changes(CouchDbClientBase dbc) {
        this.dbc = dbc;
        this.gson = dbc.getGson();
        this.uriBuilder = URIBuilder.buildUri(dbc.getDBUri()).path("_changes");
    }

    /**
     * Requests Change notifications of feed type continuous.
     * <p>
     * Feed notifications are accessed in an <i>iterator</i> style.
     * 
     * @return {@link Changes}
     */
    public Changes continuousChanges() {
        final URI uri = uriBuilder.query("feed", "continuous").build();
        if (selector == null) {
            final HttpGet get = new HttpGet(uri);
            httpRequest = get;
            final InputStream in = dbc.get(get);
            final InputStreamReader is = new InputStreamReader(in, Charsets.UTF_8);
            setReader(new BufferedReader(is));
        } else {
            final HttpPost post = new HttpPost(uri);
            httpRequest = post;
            final InputStream in = dbc.post(post, selector);
            final InputStreamReader is = new InputStreamReader(in, Charsets.UTF_8);
            setReader(new BufferedReader(is));
        }
        return this;
    }

    /**
     * Checks whether a feed is available in the continuous stream, blocking until a feed is received.
     * 
     * @return true If a feed is available
     */
    public boolean hasNext() {
        return readNextRow();
    }

    /**
     * @return The next feed in the stream.
     */
    public Row next() {
        return getNextRow();
    }

    /**
     * Stops a running continuous feed.
     */
    public void stop() {
        stop = true;
    }

    /**
     * Requests Change notifications of feed type normal.
     * 
     * @return {@link ChangesResult}
     */
    public ChangesResult getChanges() {
        final URI uri = uriBuilder.query("feed", "normal").build();
        if (selector == null && docIds == null) {
            return dbc.get(uri, ChangesResult.class);
        } else {
            String json = selector;
            if (docIds != null) {
                JsonObject docIdsJson = new JsonObject();
                JsonArray jArray = new JsonArray();
                for (String id : docIds) {
                    jArray.add(id);
                }
                docIdsJson.add("doc_ids", jArray);
                json = docIdsJson.toString();
            }

            return dbc.post(uri, json, ChangesResult.class);
        }
    }

    // Query Params

    public Changes since(String since) {
        uriBuilder.query("since", since);
        return this;
    }

    public Changes limit(int limit) {
        uriBuilder.query("limit", limit);
        return this;
    }

    public Changes heartBeat(long heartBeat) {
        uriBuilder.query("heartbeat", heartBeat);
        return this;
    }

    public Changes timeout(long timeout) {
        uriBuilder.query("timeout", timeout);
        return this;
    }

    public Changes filter(String filter) {
        if (docIds!=null || selector != null) {
            throw new IllegalArgumentException("Filter is not compatible with selector or docIds filters");
        }
        uriBuilder.query("filter", filter);
        this.filter=filter;
        return this;
    }

    public Changes selector(String json) {
        if (docIds!=null || filter != null) {
            throw new IllegalArgumentException("Selector is not compatible with filters or docIds filters");
        }
        uriBuilder.query("filter", "_selector");
        this.selector = json;
        return this;
    }

    public Changes docIds(List<String> docIds) {
        if (selector!=null || filter != null) {
            throw new IllegalArgumentException("DocIds filter is not compatible with filter or selector");
        }
        uriBuilder.query("filter", "_doc_ids");
        this.docIds = docIds;
        return this;
    }

    public Changes includeDocs(boolean includeDocs) {
        uriBuilder.query("include_docs", includeDocs);
        return this;
    }

    public Changes style(String style) {
        uriBuilder.query("style", style);
        return this;
    }

    public Changes seqInterval(long batchSize) {
        uriBuilder.query("seq_interval", batchSize);
        return this;
    }

    // Helper

    /**
     * Reads and sets the next feed in the stream.
     */
    private boolean readNextRow() {
        boolean hasNext = false;
        try {
            if (!stop) {
                String row = "";
                do {
                    row = getReader().readLine();
                } while (row.length() == 0 && !stop);

                if (!stop) {
                    if (!row.startsWith("{\"last_seq\":")) {
                        setNextRow(gson.fromJson(row, Row.class));
                        hasNext = true;
                    }
                }
            }
        } catch (Exception e) {
            terminate();
            throw new CouchDbException("Error reading continuous stream.", e);
        }
        if (!hasNext)
            terminate();
        return hasNext;
    }

    private BufferedReader getReader() {
        return reader;
    }

    private void setReader(BufferedReader reader) {
        this.reader = reader;
    }

    private Row getNextRow() {
        return nextRow;
    }

    private void setNextRow(Row nextRow) {
        this.nextRow = nextRow;
    }

    private void terminate() {
        httpRequest.abort();
        CouchDbUtil.close(getReader());
    }
}
