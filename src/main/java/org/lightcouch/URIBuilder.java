/*
 * Copyright (C) 2011 lightcouch.org
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

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;

/**
 * Helper class for construction of HTTP request URIs.
 * @since 0.0.2
 * @author Ahmed Yehia
 * 
 */
public class URIBuilder {
	private String scheme;
	private String host;
	private int port;
	private String path = "";
	private final List<String> params = new ArrayList<String>();
    private String user;
    private String password;
	
	public static URIBuilder buildUri() {
		return new URIBuilder();
	}

	public static URIBuilder buildUri(URI uri) {
		URIBuilder builder = URIBuilder.buildUri().scheme(uri.getScheme()).
				host(uri.getHost()).port(uri.getPort()).path(uri.getPath());
		return builder;
	}

	public URIBuilder scheme(String scheme) {
		this.scheme = scheme;
		return this;
	}

	public URIBuilder host(String host) {
		this.host = host;
		return this;
	}

	public URIBuilder port(int port) {
		this.port = port;
		return this;
	}
	
	public URIBuilder path(String path) {
		this.path += path;
		return this;
	}

	public URIBuilder pathEncoded(String path) {
		try {
			this.path += URLEncoder.encode(path, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			throw new IllegalArgumentException(e);
		}
		return this;
	}

	public URIBuilder query(String name, Object value) {
		if (name != null && value != null) {
			try {
				name = URLEncoder.encode(name, "UTF-8");
				value = URLEncoder.encode(String.valueOf(value), "UTF-8");
				this.params.add(String.format("%s=%s", name, value));
			} catch (UnsupportedEncodingException e) {
				throw new IllegalArgumentException(e);
			}
		}
		return this;
	}

	public URIBuilder query(Params params) {
		if (params.getParams() != null)
			this.params.addAll(params.getParams());
		return this;
	}

	public URIBuilder user(String user) {
        this.user = user;
        return this;
    }
	
	public URIBuilder password(String password) {
        this.password = password;
        return this;
    }
	
	public URI build() {
	    return build(false);
	}
	
	public URI buildWithCredentials() {
        return build(true);
    }
	
	private URI build(boolean includeCredentials) {
		final StringBuilder query = new StringBuilder();

		for (int i = 0; i < params.size(); i++) {
			String amp = (i != params.size() - 1) ? "&" : "";
			query.append(params.get(i) + amp);
		}

		String q = (query.length() == 0) ? "" : "?" + query;
		String uri = "";
		if (includeCredentials && user != null && password != null) {
			uri = String.format("%s://%s:%s@%s:%d%s%s", scheme, user, password, host, port, path, q);
		} else {
			uri = String.format("%s://%s:%d%s%s", scheme, host, port, path, q);
		}
		try {
			return new URI(uri);
		} catch (URISyntaxException e) {
			throw new IllegalArgumentException(e);
		}
	}
}
