/*
 * Copyright (C) 2018 indaba.es
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.lightcouch;

import static org.lightcouch.CouchDbUtil.assertNotEmpty;
import static org.lightcouch.CouchDbUtil.close;
import static org.lightcouch.CouchDbUtil.getAsString;
import static org.lightcouch.URIBuilder.buildUri;

import java.net.URI;
import java.util.List;

import org.apache.http.HttpResponse;

import com.google.gson.JsonObject;

public class Local {

    private static final String LOCAL_PATH = "_local";

    private CouchDbClientBase dbc;
    private URI dbURI;
    private View localDocsView;

    Local(CouchDbClientBase dbc) {
        this.dbc = dbc;
        dbURI = buildUri(dbc.getDBUri()).path(LOCAL_PATH).path("/").build();
        localDocsView = new View(dbc, "_local_docs");
    }

    public View localDocs() {
        return localDocsView;
    }

    public List<JsonObject> findAll() {
        return findAll(JsonObject.class);
    }

    public <T> List<T> findAll(Class<T> classType) {
        assertNotEmpty(classType, "Class");
        return (List<T>) localDocsView.includeDocs(true).query(classType);
    }

    /**
     * Finds JSON Object.
     * 
     * @param id The document id.
     * @return An JSON object.
     * @throws NoDocumentException If the document is not found in the database.
     */
    public JsonObject find(String id) {
        return find(JsonObject.class, id);
    }

    /**
     * Finds an Object of the specified type.
     * 
     * @param <T> Object type.
     * @param classType The class of type T.
     * @param id The document id.
     * @return An object of type T.
     * @throws NoDocumentException If the document is not found in the database.
     */
    public <T> T find(Class<T> classType, String id) {
        assertNotEmpty(classType, "Class");
        assertNotEmpty(id, "id");
        final URI uri = buildURIforLocal(id);
        return dbc.get(uri, classType);
    }

    /**
     * Checks if a document exist in the database.
     * 
     * @param id The document _id field.
     * @return true If the document is found, false otherwise.
     */
    public boolean contains(String id) {
        assertNotEmpty(id, "id");
        HttpResponse response = null;
        try {
            final URI uri = buildURIforLocal(id);
            response = dbc.head(uri);
        } catch (NoDocumentException e) {
            return false;
        } finally {
            close(response);
        }
        return true;
    }

    public Response save(Object object) {
        return dbc.put(dbURI, object, true);
    }

    public Response update(Object object) {

        final JsonObject json = dbc.getGson().toJsonTree(object).getAsJsonObject();
        String id = CouchDbUtil.getAsString(json, "_id");
        URI baseURI = buildURIforLocal(id, false);
        return dbc.put(baseURI, object, false);
    }

    /**
     * Removes a document from the database.
     * <p>
     * The object must have the correct <code>_id</code> and <code>_rev</code> values.
     * 
     * @param object The document to remove as object.
     * @throws NoDocumentException If the document is not found in the database.
     * @return {@link Response}
     */
    public Response remove(Object object) {
        assertNotEmpty(object, "object");
        JsonObject jsonObject = dbc.getGson().toJsonTree(object).getAsJsonObject();
        final String id = getAsString(jsonObject, "_id");
        return remove(id);
    }

    public Response removeWithRev(Object object) {
        assertNotEmpty(object, "object");
        JsonObject jsonObject = dbc.getGson().toJsonTree(object).getAsJsonObject();
        final String id = getAsString(jsonObject, "_id");
        final String rev = getAsString(jsonObject, "_rev");
        return remove(id, rev);
    }

    public Response remove(String id) {
        assertNotEmpty(id, "id");
        final URI docURI = buildURIforLocal(id);
        return dbc.delete(docURI);
    }

    public Response remove(String id, String rev) {
        assertNotEmpty(id, "id");
        assertNotEmpty(id, "rev");
        final URI docURI = buildUri(buildURIforLocal(id)).query("rev", rev).build();
        return dbc.delete(docURI);
    }

    private URI buildURIforLocal(String id) {

        return buildURIforLocal(id, true);
    }

    private URI buildURIforLocal(String id, boolean includeId) {
        URI baseURI = dbURI;
        if (id.startsWith(LOCAL_PATH)) {
            baseURI = dbc.getDBUri();
        }
        if (includeId) {
            return buildUri(baseURI).pathEncoded(id).build();
        } else {
            return buildUri(baseURI).build();
        }
    }
}
