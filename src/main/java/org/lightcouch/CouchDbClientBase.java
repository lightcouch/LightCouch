/*
 * Copyright (C) 2011 Ahmed Yehia (ahmed.yehia.m@gmail.com)
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

import static java.lang.String.format;
import static org.lightcouch.CouchDbUtil.*;
import static org.lightcouch.URIBuilder.builder;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Type;
import java.net.URI;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.List;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import ch.boye.httpclientandroidlib.androidextra.HttpClientAndroidLog;

import ch.boye.httpclientandroidlib.HttpHost;
import ch.boye.httpclientandroidlib.HttpRequest;
import ch.boye.httpclientandroidlib.HttpRequestInterceptor;
import ch.boye.httpclientandroidlib.HttpResponse;
import ch.boye.httpclientandroidlib.HttpResponseInterceptor;
import ch.boye.httpclientandroidlib.HttpStatus;
import ch.boye.httpclientandroidlib.auth.AuthScope;
import ch.boye.httpclientandroidlib.auth.UsernamePasswordCredentials;
import ch.boye.httpclientandroidlib.client.AuthCache;
import ch.boye.httpclientandroidlib.client.HttpClient;
import ch.boye.httpclientandroidlib.client.methods.HttpDelete;
import ch.boye.httpclientandroidlib.client.methods.HttpEntityEnclosingRequestBase;
import ch.boye.httpclientandroidlib.client.methods.HttpGet;
import ch.boye.httpclientandroidlib.client.methods.HttpHead;
import ch.boye.httpclientandroidlib.client.methods.HttpPost;
import ch.boye.httpclientandroidlib.client.methods.HttpPut;
import ch.boye.httpclientandroidlib.client.methods.HttpRequestBase;
import ch.boye.httpclientandroidlib.client.protocol.ClientContext;
import ch.boye.httpclientandroidlib.conn.params.ConnRoutePNames;
import ch.boye.httpclientandroidlib.conn.scheme.PlainSocketFactory;
import ch.boye.httpclientandroidlib.conn.scheme.Scheme;
import ch.boye.httpclientandroidlib.conn.scheme.SchemeRegistry;
import ch.boye.httpclientandroidlib.conn.scheme.SchemeSocketFactory;
import ch.boye.httpclientandroidlib.conn.ssl.SSLSocketFactory;
import ch.boye.httpclientandroidlib.entity.InputStreamEntity;
import ch.boye.httpclientandroidlib.entity.StringEntity;
import ch.boye.httpclientandroidlib.impl.auth.BasicScheme;
import ch.boye.httpclientandroidlib.impl.client.BasicAuthCache;
import ch.boye.httpclientandroidlib.impl.client.DefaultHttpClient;
import ch.boye.httpclientandroidlib.impl.conn.PoolingClientConnectionManager;
import ch.boye.httpclientandroidlib.params.CoreConnectionPNames;
import ch.boye.httpclientandroidlib.params.CoreProtocolPNames;
import ch.boye.httpclientandroidlib.protocol.BasicHttpContext;
import ch.boye.httpclientandroidlib.protocol.HttpContext;
import ch.boye.httpclientandroidlib.util.EntityUtils;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.google.gson.reflect.TypeToken;

/**
 * Base client class to be extended by a concrete subclass, responsible for establishing
 * a connection with the database and the definition of the basic HTTP request handling and validation. 
 * @see CouchDbClient
 * @author Ahmed Yehia
 */
abstract class CouchDbClientBase {

    static final HttpClientAndroidLog log = new HttpClientAndroidLog(CouchDbClientBase.class);

	private HttpClient httpClient;
	private URI baseURI;
	private URI dbURI;
	private Gson gson; 
	private CouchDbConfig config;
	
	private HttpHost host;
	private BasicHttpContext context;
	
	protected CouchDbClientBase() {
		this(new CouchDbConfig());
	}
	
	protected CouchDbClientBase(CouchDbConfig config) {
		CouchDbProperties props = config.getProperties();
		this.httpClient = createHttpClient(props);
		this.gson = initGson(new GsonBuilder());
		this.config = config;
		baseURI = builder().scheme(props.getProtocol()).host(props.getHost()).port(props.getPort()).path("/").build();
		dbURI   = builder(baseURI).path(props.getDbName()).path("/").build();
	}
	
	// ---------------------------------------------- Getters
	
	/**
	 * @return The database URI.
	 */
	protected URI getDBUri() {
		return dbURI;
	}
	
	protected URI getBaseUri() {
		return baseURI;
	}
	
	protected Gson getGson() {
		return gson;
	}
	
	protected CouchDbConfig getConfig() {
		return config;
	}
	
	// HTTP Requests
	
	/**
	 * Performs a HTTP GET request. 
	 * @return {@link InputStream} 
	 */
	InputStream get(HttpGet httpGet) {
		HttpResponse response = executeRequest(httpGet); 
		return getStream(response);
	}
	
	/**
	 * Performs a HTTP GET request. 
	 * @return {@link InputStream} 
	 */
	InputStream get(URI uri) {
		HttpGet get = new HttpGet(uri);
		get.addHeader("Accept", "application/json");
		return get(get);
	}
	
	/**
	 * Performs a HTTP GET request. 
	 * @return An object of type T
	 */
	<T> T get(URI uri, Class<T> classType) {
		InputStream instream = null;
		try {
			instream = get(uri);
			return deserialize(instream, classType);
		} finally {
			close(instream);
		}
	}
	
	/**
	 * Performs a HTTP HEAD request. 
	 * @return {@link HttpResponse}
	 */
	HttpResponse head(URI uri) {
		return executeRequest(new HttpHead(uri));
	}
	
	/**
	 * Performs a HTTP PUT request, saves or updates a document.
	 * @return {@link Response}
	 */
	Response put(URI uri, Object object, boolean newEntity) {
		assertNotEmpty(object, "object");
		HttpResponse response = null;
		try {  
			JsonObject json = getGson().toJsonTree(object).getAsJsonObject();
			String id = getElement(json, "_id");
			String rev = getElement(json, "_rev");
			if(newEntity) { // save
				assertNull(rev, "revision");
				id = (id == null) ? generateUUID() : id;
			} else { // update
				assertNotEmpty(id, "id");
				assertNotEmpty(rev, "revision");
			}
			HttpPut put = new HttpPut(builder(uri).path(id).build());
			setEntity(put, json.toString());
			response = executeRequest(put); 
			return getResponse(response);
		} finally {
			close(response);
		}
	}
	
	/**
	 * Performs a HTTP PUT request, saves an attachment.
	 * @return {@link Response}
	 */
	Response put(URI uri, InputStream instream, String contentType) {
		HttpResponse response = null;
		try {
			HttpPut httpPut = new HttpPut(uri);
			InputStreamEntity entity = new InputStreamEntity(instream, -1);
			entity.setContentType(contentType);
			httpPut.setEntity(entity);
			response = executeRequest(httpPut);
			return getResponse(response);
		} finally {
			close(response);
		}
	}
	
	/**
	 * Performs a HTTP POST request.
	 * @return {@link HttpResponse}
	 */
	HttpResponse post(URI uri, String json) {
		HttpPost post = new HttpPost(uri);
		setEntity(post, json);
		return executeRequest(post);
	}
	
	/**
	 * Performs a HTTP DELETE request.
	 * @return {@link Response}
	 */
	Response delete(URI uri) {
		HttpResponse response = null;
		try {
			HttpDelete delete = new HttpDelete(uri);
			response = executeRequest(delete); 
			return getResponse(response);
		} finally {
			close(response);
		}
	}
	
	/**
	 * Executes a HTTP request.
	 * @param request The HTTP request to execute.
	 * @return {@link HttpResponse}
	 */
	protected HttpResponse executeRequest(HttpRequestBase request) {
		HttpResponse response = null;
		try {
            if(log.isTraceEnabled()) {
                logHttpRequest(request);
            }

            response = httpClient.execute(host, request, context);

            if(log.isTraceEnabled()) {
                logHttpResponse(response);
            }
		} catch (IOException e) {
			request.abort();
			log.error("Error executing request. " + e.getMessage());
			throw new CouchDbException(e);
		} 
		return response;
	}

    private void logHttpRequest(HttpRequestBase request) {
        log.trace("Executing request: " + request );

    }

    private void logHttpResponse(HttpResponse response) {
        log.trace("Response: " + response);
    }

    // Helpers
	
	/**
	 * @return {@link DefaultHttpClient} instance.
	 */
	private HttpClient createHttpClient(CouchDbProperties props) {
		DefaultHttpClient httpclient = null;
		try {
			SchemeSocketFactory ssf = null;
			if(props.getProtocol().equals("https")) {
				TrustManager trustManager = new X509TrustManager() {
					public void checkClientTrusted(X509Certificate[] chain,
							String authType) throws CertificateException {
					}
					public void checkServerTrusted(X509Certificate[] chain,
							String authType) throws CertificateException {
					}
					public X509Certificate[] getAcceptedIssuers() {
						return null;
					}
				};
				SSLContext sslcontext = SSLContext.getInstance("TLS");
				sslcontext.init(null, new TrustManager[] { trustManager }, null);
				ssf = new SSLSocketFactory(sslcontext, SSLSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER); 
				SSLSocket socket = (SSLSocket) ssf.createSocket(null); 
				socket.setEnabledCipherSuites(new String[] { "SSL_RSA_WITH_RC4_128_MD5" });
			} else {
				ssf = PlainSocketFactory.getSocketFactory();
			}
			SchemeRegistry schemeRegistry = new SchemeRegistry();
			schemeRegistry.register(new Scheme(props.getProtocol(), props.getPort(), ssf));
			PoolingClientConnectionManager ccm = new PoolingClientConnectionManager(schemeRegistry);
			httpclient = new DefaultHttpClient(ccm);
			host = new HttpHost(props.getHost(), props.getPort(), props.getProtocol());
			context = new BasicHttpContext();
			// Http params
			httpclient.getParams().setParameter(CoreProtocolPNames.HTTP_CONTENT_CHARSET, "UTF-8");
			httpclient.getParams().setParameter(CoreConnectionPNames.SO_TIMEOUT, props.getSocketTimeout());
			httpclient.getParams().setParameter(CoreConnectionPNames.CONNECTION_TIMEOUT, props.getConnectionTimeout());
			int maxConnections = props.getMaxConnections();
			if(maxConnections != 0) {
				ccm.setMaxTotal(maxConnections);
				ccm.setDefaultMaxPerRoute(maxConnections);
			}
			if(props.getProxyHost() != null) {
				HttpHost proxy = new HttpHost(props.getProxyHost(), props.getProxyPort());
				httpclient.getParams().setParameter(ConnRoutePNames.DEFAULT_PROXY, proxy);
			}
			// basic authentication
			if(props.getUsername() != null && props.getPassword() != null) {
				httpclient.getCredentialsProvider().setCredentials(
						new AuthScope(props.getHost(), props.getPort()),
						new UsernamePasswordCredentials(props.getUsername(), props.getPassword()));
				props.clearPassword();
				AuthCache authCache = new BasicAuthCache();
				BasicScheme basicAuth = new BasicScheme();
				authCache.put(host, basicAuth);
				context.setAttribute(ClientContext.AUTH_CACHE, authCache);
			}
			// request interceptor
			httpclient.addRequestInterceptor(new HttpRequestInterceptor() {
                public void process(
                        final HttpRequest request,
                        final HttpContext context) throws IOException {
                	if(log.isInfoEnabled()) 
        				log.info(">> " + request.getRequestLine());
                }
            });
			// response interceptor
			httpclient.addResponseInterceptor(new HttpResponseInterceptor() {
                public void process(
                        final HttpResponse response,
                        final HttpContext context) throws IOException {
                	validate(response);
                	if(log.isInfoEnabled())
        				log.info("<< Status: " + response.getStatusLine().getStatusCode());
                }
            });
		} catch (Exception e) {
			log.error("Error Creating HTTP client. " + e.getMessage());
			throw new IllegalStateException(e);
		}
		return httpclient;
	}
	
	/**
	 * Validates a HTTP response; on error cases logs status and throws relevant exceptions.
	 * @param response The HTTP response.
	 */
	private void validate(HttpResponse response) throws IOException {
		int code = response.getStatusLine().getStatusCode();
		if(code == 200 || code == 201 || code == 202) { // success (ok | created | accepted)
			return;
		} 
		String msg = format("<< Status: %s (%s) ", code, response.getStatusLine().getReasonPhrase());
		switch (code) {
		case HttpStatus.SC_NOT_FOUND: {
			log.info(msg); 
			throw new NoDocumentException(msg);
		}
		case HttpStatus.SC_CONFLICT: {
			log.warn(msg);
			throw new DocumentConflictException(msg);
		}
		default: { // other errors: 400 | 401 | 500 etc.
			log.error(msg += EntityUtils.toString(response.getEntity()));
			throw new CouchDbException(msg);
		}
		}
	}
	
	/**
	 * @param response The {@link HttpResponse}
	 * @return {@link Response}
	 */
	Response getResponse(HttpResponse response) throws CouchDbException {
		return deserialize(getStream(response), Response.class);
	}
	
	/**
	 * @param response The {@link HttpResponse}
	 * @return {@link Response}
	 */
	List<Response> getResponseList(HttpResponse response) throws CouchDbException {
		InputStream instream = getStream(response);
		Reader reader = new InputStreamReader(instream);
		return getGson().fromJson(reader, new TypeToken<List<Response>>(){}.getType());
	}
	
	/**
	 * Sets a JSON String as a request entity.
	 * @param httpRequest The request to set entity.
	 * @param json The JSON String to set.
	 */
	protected void setEntity(HttpEntityEnclosingRequestBase httpRequest, String json) {
		try {
			StringEntity entity = new StringEntity(json, "UTF-8");
			entity.setContentType("application/json");
			httpRequest.setEntity(entity);
		} catch (UnsupportedEncodingException e) {
			log.error("Error setting request data. " + e.getMessage());
			throw new IllegalArgumentException(e);
		}
	}
	
	/**
	 * @return {@link InputStream} from a {@link HttpResponse}
	 */
	InputStream getStream(HttpResponse response) {
		try { 
			return response.getEntity().getContent();
		} catch (Exception e) {
			log.error("Error reading response. " + e.getMessage());
			throw new CouchDbException(e);
		}
	}
	
	<T> T deserialize(InputStream instream, Class<T> classType) {
		Reader reader = new InputStreamReader(instream);
		return getGson().fromJson(reader, classType);
	}
	
	/**
	 * <p>The supplied {@link GsonBuilder} is used to create a new {@link Gson} instance.
	 * Useful for registering custom serializers/deserializers, for example JodaTime DateTime class.
	 */
	protected void setGsonBuilder(GsonBuilder gsonBuilder) {
		this.gson = initGson(gsonBuilder);
	}
	
	/**
	 * Builds {@link Gson} and registers any required serializer/deserializer.
	 * @return {@link Gson} instance
	 */
	private Gson initGson(GsonBuilder gsonBuilder) {
		gsonBuilder.registerTypeAdapter(JsonObject.class, new JsonDeserializer<JsonObject>() {
			public JsonObject deserialize(JsonElement json,
					Type typeOfT, JsonDeserializationContext context)
					throws JsonParseException {
				return json.getAsJsonObject();
			}
		});
		gsonBuilder.registerTypeAdapter(JsonObject.class, new JsonSerializer<JsonObject>() {
			public JsonElement serialize(JsonObject src, Type typeOfSrc,
					JsonSerializationContext context) {
				return src.getAsJsonObject();
			}
			
		});
		return gsonBuilder.create();
	}
	
	/**
	 * Shuts down the connection manager used by this client instance.
	 */
	protected void shutdown() {
		this.httpClient.getConnectionManager().shutdown();
	}
}
