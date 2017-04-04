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

import org.apache.http.NameValuePair;
import org.apache.http.message.BasicNameValuePair;

import java.util.ArrayList;
import java.util.List;

/**
 * Query parameters to append to find requests.
 * <p>Example: 
 * <pre>
 * dbClient.find(Foo.class, "doc-id", new Params().revsInfo().attachments());
 * </pre>
 * @see CouchDbClientBase#find(Class, String, Params)
 * @since 0.0.6
 * @author Ahmed Yehia
 * 
 */
public class Params {

	private List<NameValuePair> params = new ArrayList<NameValuePair>();

	public Params revsInfo() {
		params.add(new BasicNameValuePair("revs_info", "true"));
		return this;
	}

	public Params attachments() {
		params.add(new BasicNameValuePair("attachments", "true"));
		return this;
	}

	public Params revisions() {
		params.add(new BasicNameValuePair("revs", "true"));
		return this;
	}

	public Params rev(String rev) {
		params.add(new BasicNameValuePair("rev", rev));
		return this;
	}

	public Params conflicts() {
		params.add(new BasicNameValuePair("conflicts", "true"));
		return this;
	}

	public Params localSeq() {
		params.add(new BasicNameValuePair("local_seq", "true"));
		return this;
	}

	public Params addParam(String name, String value) {
		params.add(new BasicNameValuePair(name, value));
		return this;
	}

	public List<NameValuePair> getParams() {
		return params.isEmpty() ? null : params;
	}
}
