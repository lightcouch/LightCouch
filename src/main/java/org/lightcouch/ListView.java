package org.lightcouch;

import static org.lightcouch.CouchDbUtil.getStream;

import java.io.InputStream;
import java.net.URI;

public class ListView extends View {
	
	private static final String listPath(String viewId, String listId) {
		if (!viewId.contains("/"))
			throw new IllegalArgumentException("view must look like 'designdoc/viewname'.");
		
		String[] parts = viewId.split("/");
		return String.format("_design/%s/_list/%s/%s", parts[0], listId, parts[1]);
	}
	
	private String mimeType = "application/json";
	

	public ListView(CouchDbClientBase dbc, String viewId, String listId, String mimeType) {
		super(dbc, viewId);

		if (mimeType != null && !mimeType.isEmpty())
			this.mimeType = mimeType;

		this.uriBuilder = URIBuilder.buildUri(dbc.getDBUri()).path(listPath(viewId, listId));
	}

	public InputStream queryForStream() {
		URI uri = uriBuilder.build();
		if(allDocsKeys != null) { // bulk docs
			return getStream(dbc.post(uri, allDocsKeys));
		}

		return dbc.get(uri, this.mimeType);
	}
}
