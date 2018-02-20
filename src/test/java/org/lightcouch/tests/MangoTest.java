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

package org.lightcouch.tests;

import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;

import java.util.List;

import org.junit.Assume;
import org.junit.Test;

public class MangoTest extends CouchDbTestBase {	
	
	@Test
	public void findDocs() {
	    
	    Assume.assumeTrue(isCouchDB2());
	    
		dbClient.save(new Foo());
		
		String jsonQuery = "{ \"selector\": { \"_id\": { \"$gt\": null } }, \"limit\":2 }";
		
		List<Foo> docs = dbClient.findDocs(jsonQuery, Foo.class);
		
		assertThat(docs.size(), not(0));
	}
}
