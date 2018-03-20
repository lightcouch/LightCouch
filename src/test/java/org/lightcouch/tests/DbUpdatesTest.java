/*
 * Copyright (C) 2011 lightcouch.org
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
package org.lightcouch.tests;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.Assume;
import org.junit.Test;
import org.lightcouch.DbUpdates;
import org.lightcouch.Response;

public class DbUpdatesTest extends CouchDbTestBase {
	
	private static final String SINCE_PARAM_NOW = "now";
	
	@Test
	public void testDbUpdates() throws InterruptedException {
	    
	    Assume.assumeTrue(isCouchDB2());
	    
		DbUpdates updates = dbClient.context().dbUpdates(SINCE_PARAM_NOW);
		
		assertNotNull(updates);
		assertTrue(updates.getResults().isEmpty());
		
		String lastSeqNow = updates.getLastSeq();
		
		Response response = dbClient.save(new Foo());
		assertNull(response.getError());
		
		updates = dbClient.context().dbUpdates(lastSeqNow);
		String lastSeq = null;
		int count = 0;
		while((lastSeq == null || lastSeqNow.equals(lastSeq)) && count < 10) {
			System.out.println("Equal sequences!");
			Thread.sleep(100);
			updates = dbClient.context().dbUpdates(lastSeqNow);
			lastSeq = updates.getLastSeq();
			count++;
		}
		
		assertFalse(lastSeqNow.equals(lastSeq));
		assertFalse(updates.getResults().isEmpty());
	}
}
